package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Cache represents the Redis cache in Argo CD
type Cache struct {
	mu                sync.RWMutex
	resolvedRevisions map[string]string // revision -> commitSHA
	manifests         map[string]string // commitSHA -> manifest
}

func NewCache() *Cache {
	return &Cache{
		resolvedRevisions: make(map[string]string),
		manifests:         make(map[string]string),
	}
}

func (c *Cache) GetRevision(revision string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sha, exists := c.resolvedRevisions[revision]
	return sha, exists
}

func (c *Cache) SetRevision(revision, sha string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolvedRevisions[revision] = sha
}

func (c *Cache) InvalidateRevision(revision string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.resolvedRevisions, revision)
}

func (c *Cache) GetManifest(sha string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	manifest, exists := c.manifests[sha]
	return manifest, exists
}

func (c *Cache) SetManifest(sha, manifest string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.manifests[sha] = manifest
}

func (c *Cache) InvalidateManifest(sha string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.manifests, sha)
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolvedRevisions = make(map[string]string)
	c.manifests = make(map[string]string)
}

// GitRepo mocks a remote Git repository
type GitRepo struct {
	mu      sync.Mutex
	commits map[string][]string // branch -> list of commit SHAs (latest is last)
}

func NewGitRepo() *GitRepo {
	return &GitRepo{
		commits: map[string][]string{
			"main": {"sha-1"},
		},
	}
}

func (g *GitRepo) PushCommit(branch, sha string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.commits[branch] = append(g.commits[branch], sha)
}

func (g *GitRepo) ResolveLatest(branch string) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	commits, exists := g.commits[branch]
	if !exists || len(commits) == 0 {
		return "", errors.New("branch not found")
	}
	return commits[len(commits)-1], nil
}

// RepoServer simulates the argocd-repo-server
type RepoServer struct {
	cache *Cache
	git   *GitRepo
}

func NewRepoServer(cache *Cache, git *GitRepo) *RepoServer {
	return &RepoServer{
		cache: cache,
		git:   git,
	}
}

// ResolveRevision resolves a revision (e.g., branch) to a commit SHA.
// If forceRefresh is true, it bypasses the cache, fetches from Git, and updates the cache.
func (r *RepoServer) ResolveRevision(ctx context.Context, revision string, forceRefresh bool) (string, error) {
	if !forceRefresh {
		if sha, found := r.cache.GetRevision(revision); found {
			log.Printf("[DEBUG] component=repo-server event=cache_hit revision=%s sha=%s", revision, sha)
			return sha, nil
		}
		log.Printf("[DEBUG] component=repo-server event=cache_miss revision=%s", revision)
	} else {
		log.Printf("[DEBUG] component=repo-server event=cache_bypass revision=%s", revision)
	}

	// Fetch from Git (simulated network call)
	sha, err := r.git.ResolveLatest(revision)
	if err != nil {
		return "", err
	}

	log.Printf("[DEBUG] component=repo-server event=resolved_revision revision=%s sha=%s source=git", revision, sha)
	r.cache.SetRevision(revision, sha)
	return sha, nil
}

// GenerateManifest generates manifests for a given commit SHA.
func (r *RepoServer) GenerateManifest(ctx context.Context, sha string, forceRefresh bool) (string, error) {
	if !forceRefresh {
		if manifest, found := r.cache.GetManifest(sha); found {
			log.Printf("[DEBUG] component=repo-server event=cache_hit manifest_sha=%s", sha)
			return manifest, nil
		}
		log.Printf("[DEBUG] component=repo-server event=cache_miss manifest_sha=%s", sha)
	} else {
		log.Printf("[DEBUG] component=repo-server event=cache_bypass manifest_sha=%s", sha)
	}

	// Simulate manifest generation
	manifest := fmt.Sprintf("manifest-for-%s", sha)
	r.cache.SetManifest(sha, manifest)
	return manifest, nil
}

// AppController simulates the argocd-application-controller
type AppController struct {
	repoServer *RepoServer
	cache      *Cache
	syncLock   sync.Mutex // Lock to prevent race condition during concurrent sync/refresh
}

func NewAppController(repoServer *RepoServer, cache *Cache) *AppController {
	return &AppController{
		repoServer: repoServer,
		cache:      cache,
	}
}

// Reconcile simulates the reconciliation loop
func (c *AppController) Reconcile(ctx context.Context, revision string, refreshType string) (string, string, error) {
	c.syncLock.Lock()
	defer c.syncLock.Unlock()

	log.Printf("[DEBUG] component=controller event=reconcile_start refresh_type=%s revision=%s", refreshType, revision)

	forceRefresh := false
	if refreshType == "normal" || refreshType == "hard" {
		forceRefresh = true
		if refreshType == "hard" {
			log.Printf("[DEBUG] component=controller event=cache_clear reason=hard_refresh")
			c.cache.Clear()
		} else {
			log.Printf("[DEBUG] component=controller event=cache_invalidate revision=%s reason=normal_refresh", revision)
			c.cache.InvalidateRevision(revision)
		}
	}

	// 1. Resolve revision
	sha, err := c.repoServer.ResolveRevision(ctx, revision, forceRefresh)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve revision: %w", err)
	}
	log.Printf("[DEBUG] component=controller event=resolved_revision revision=%s sha=%s", revision, sha)

	// 2. Generate manifest
	manifest, err := c.repoServer.GenerateManifest(ctx, sha, forceRefresh)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate manifest: %w", err)
	}

	log.Printf("[DEBUG] component=controller event=reconcile_complete sha=%s manifest=%s", sha, manifest)
	return sha, manifest, nil
}

// HandleWebhook simulates a Git webhook event triggering cache invalidation and reconciliation
func (c *AppController) HandleWebhook(ctx context.Context, revision string, newSHA string, git *GitRepo) {
	c.syncLock.Lock()
	defer c.syncLock.Unlock()

	log.Printf("[DEBUG] component=webhook event=received revision=%s new_sha=%s", revision, newSHA)
	git.PushCommit(revision, newSHA)

	// Invalidate the cache immediately to prevent stale reads
	c.cache.InvalidateRevision(revision)
	log.Printf("[DEBUG] component=webhook event=cache_invalidate revision=%s", revision)
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	fmt.Println("Starting Argo CD Race Condition Fix Simulation...")

	cache := NewCache()
	git := NewGitRepo()
	repoServer := NewRepoServer(cache, git)
	controller := NewAppController(repoServer, cache)

	ctx := context.Background()

	// 1. Initial reconciliation
	log.Println("--- Initial Reconciliation ---")
	sha1, _, _ := controller.Reconcile(ctx, "main", "")
	fmt.Printf("Resolved SHA: %s\n\n", sha1)

	// 2. Subsequent reconciliation (should hit cache)
	log.Println("--- Subsequent Reconciliation (Cache Hit) ---")
	sha2, _, _ := controller.Reconcile(ctx, "main", "")
	fmt.Printf("Resolved SHA: %s\n\n", sha2)

	// 3. Webhook event arrives concurrently with a sync request
	log.Println("--- Simulating Concurrent Webhook and Sync ---")
	var wg sync.WaitGroup
	wg.Add(2)

	// Webhook goroutine
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // simulate slight delay
		controller.HandleWebhook(ctx, "main", "sha-2", git)
	}()

	// Sync/Reconcile goroutine with normal refresh
	var finalSHA string
	go func() {
		defer wg.Done()
		// Wait a bit to ensure webhook starts or is about to start
		time.Sleep(15 * time.Millisecond)
		sha, _, _ := controller.Reconcile(ctx, "main", "normal")
		finalSHA = sha
	}()

	wg.Wait()
	fmt.Printf("Final Resolved SHA after webhook and refresh: %s\n\n", finalSHA)

	if finalSHA == "sha-2" {
		fmt.Println("SUCCESS: Race condition resolved! The controller resolved the latest commit SHA post-refresh.")
	} else {
		fmt.Println("FAILURE: Stale SHA resolved.")
	}
}
