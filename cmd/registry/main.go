package main

import (
	_ "net/http/pprof"

	"github.com/tonyhb/distribution/registry"
	_ "github.com/tonyhb/distribution/registry/auth/htpasswd"
	_ "github.com/tonyhb/distribution/registry/auth/silly"
	_ "github.com/tonyhb/distribution/registry/auth/token"
	_ "github.com/tonyhb/distribution/registry/proxy"
	_ "github.com/tonyhb/distribution/registry/storage/driver/azure"
	_ "github.com/tonyhb/distribution/registry/storage/driver/filesystem"
	_ "github.com/tonyhb/distribution/registry/storage/driver/gcs"
	_ "github.com/tonyhb/distribution/registry/storage/driver/inmemory"
	_ "github.com/tonyhb/distribution/registry/storage/driver/middleware/cloudfront"
	_ "github.com/tonyhb/distribution/registry/storage/driver/middleware/redirect"
	_ "github.com/tonyhb/distribution/registry/storage/driver/oss"
	_ "github.com/tonyhb/distribution/registry/storage/driver/s3-aws"
	_ "github.com/tonyhb/distribution/registry/storage/driver/s3-goamz"
	_ "github.com/tonyhb/distribution/registry/storage/driver/swift"
)

func main() {
	registry.RootCmd.Execute()
}
