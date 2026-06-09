package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRaceConditionResolution(t *testing.T) {
	cache := NewCache()
	git := NewGitRepo()
	repoServer := NewRepoServer(cache, git)
	controller := NewAppController(repoServer, cache)
	ctx := context.Background()

	// 1. Initial resolve
	sha1, _, err := controller.Reconcile(ctx, "main", "")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if sha1 != "sha-1" {
		t.Errorf("Expected sha-1, got %s", sha1)
	}

	// 2. Simulate concurrent webhook and refresh/sync
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		controller.HandleWebhook(ctx, "main", "sha-2", git)
	}()

	var finalSHA string
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		sha, _, err := controller.Reconcile(ctx, "main", "normal")
		if err != nil {
			t.Errorf("Reconcile error: %v", err)
		}
		finalSHA = sha
	}()

	wg.Wait()

	if finalSHA != "sha-2" {
		t.Errorf("Expected final SHA to be sha-2, got %s", finalSHA)
	}
}
