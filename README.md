# Worker Service

## Make
### Build
```bash
make -f ./scripts/Makefile build
```

### Run
```bash
make -f ./scripts/Makefile run
```

### Clean
```bash
make -f ./scripts/Makefile clean
```

## Docker

### Build
```bash
docker build -t worker-service -f ./.docker/Dockerfile .
```

### Run
```bash
docker run worker-service
```