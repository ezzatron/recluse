DOCKER_REPO := ezzatron/recluse/example-bank

-include .makefiles/Makefile
-include .makefiles/pkg/docker/v1/Makefile
-include .makefiles/pkg/js/v1/Makefile
-include .makefiles/pkg/js/v1/with-yarn.mk

.makefiles/%:
	@curl -sfL https://makefiles.dev/v1 | bash /dev/stdin "$@"
