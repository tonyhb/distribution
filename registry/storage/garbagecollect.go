package storage

import (
	"fmt"

	"github.com/tonyhb/distribution"
	"github.com/tonyhb/distribution/context"
	"github.com/tonyhb/distribution/digest"
	"github.com/tonyhb/distribution/manifest/schema2"
	"github.com/tonyhb/distribution/reference"
	"github.com/tonyhb/distribution/registry/storage/driver"
)

func emit(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
}

// MarkAndSweep performs a mark and sweep of registry data
func MarkAndSweep(ctx context.Context, storageDriver driver.StorageDriver, registry distribution.Namespace, dryRun bool) error {
	repositoryEnumerator, ok := registry.(distribution.RepositoryEnumerator)
	if !ok {
		return fmt.Errorf("unable to convert Namespace to RepositoryEnumerator")
	}

	// mark
	markSet := make(map[digest.Digest]struct{})
	err := repositoryEnumerator.Enumerate(ctx, func(repoName string) error {
		if dryRun {
			emit(repoName)
		}

		var err error
		named, err := reference.ParseNamed(repoName)
		if err != nil {
			return fmt.Errorf("failed to parse repo name %s: %v", repoName, err)
		}
		repository, err := registry.Repository(ctx, named)
		if err != nil {
			return fmt.Errorf("failed to construct repository: %v", err)
		}

		manifestService, err := repository.Manifests(ctx)
		if err != nil {
			return fmt.Errorf("failed to construct manifest service: %v", err)
		}

		manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
		if !ok {
			return fmt.Errorf("unable to convert ManifestService into ManifestEnumerator")
		}

		err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
			// Mark the manifest's blob
			if dryRun {
				emit("%s: marking manifest %s ", repoName, dgst)
			}
			markSet[dgst] = struct{}{}

			manifest, err := manifestService.Get(ctx, dgst)
			if err != nil {
				return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
			}

			descriptors := manifest.References()
			for _, descriptor := range descriptors {
				markSet[descriptor.Digest] = struct{}{}
				if dryRun {
					emit("%s: marking blob %s", repoName, descriptor.Digest)
				}
			}

			switch manifest.(type) {
			case *schema2.DeserializedManifest:
				config := manifest.(*schema2.DeserializedManifest).Config
				if dryRun {
					emit("%s: marking configuration %s", repoName, config.Digest)
				}
				markSet[config.Digest] = struct{}{}
				break
			}

			return nil
		})

		if err != nil {
			// In certain situations such as unfinished uploads, deleting all
			// tags in S3 or removing the _manifests folder manually, this
			// error may be of type PathNotFound.
			//
			// In these cases we can continue marking other manifests safely.
			if _, ok := err.(driver.PathNotFoundError); ok {
				return nil
			}
		}

		return err
	})

	if err != nil {
		return fmt.Errorf("failed to mark: %v\n", err)
	}

	// sweep
	blobService := registry.Blobs()
	deleteSet := make(map[digest.Digest]struct{})
	err = blobService.Enumerate(ctx, func(dgst digest.Digest) error {
		// check if digest is in markSet. If not, delete it!
		if _, ok := markSet[dgst]; !ok {
			deleteSet[dgst] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error enumerating blobs: %v", err)
	}
	if dryRun {
		emit("\n%d blobs marked, %d blobs eligible for deletion", len(markSet), len(deleteSet))
	}
	// Construct vacuum
	vacuum := NewVacuum(ctx, storageDriver)
	for dgst := range deleteSet {
		if dryRun {
			emit("blob eligible for deletion: %s", dgst)
			continue
		}
		err = vacuum.RemoveBlob(string(dgst))
		if err != nil {
			return fmt.Errorf("failed to delete blob %s: %v\n", dgst, err)
		}
	}

	return err
}
