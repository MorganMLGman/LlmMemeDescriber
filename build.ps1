param(
    [string]$ImageName = "llm-meme-describer",
    [string]$Tag = "latest",
    [string]$Platforms = "linux/amd64,linux/arm64,linux/arm/v7",
    [string]$CacheMode = "local", # local or registry
    [string]$CacheRef = "", # when CacheMode=registry provide <repo>/cache:tag
    [switch]$Push
)

# Enable BuildKit
$env:DOCKER_BUILDKIT = "1"

Write-Host "Building multi-arch image $($ImageName):$($Tag) for platforms: $Platforms"

$builder = docker buildx ls | Select-String -Pattern "default" -Quiet
if (-not $builder) {
    Write-Host "Creating a buildx builder..."
    docker buildx create --use --name multiarch-builder | Out-Null
} else {
    docker buildx use multiarch-builder | Out-Null
}

docker run --rm --privileged tonistiigi/binfmt --install all

$pushFlag = ""
if ($Push) { $pushFlag = "--push" } else { $pushFlag = "--load" }

# Prepare cache options
$cacheOptions = ""
if ($CacheMode -eq 'local') {
    $cacheDir = "${PWD}\\.buildx-cache"
    $cacheOptions = "--cache-to=type=local,dest=$cacheDir,mode=max --cache-from=type=local,src=$cacheDir"
} elseif ($CacheMode -eq 'registry' -and $CacheRef -ne '') {
    $cacheOptions = "--cache-to=type=registry,ref=$CacheRef,mode=max --cache-from=type=registry,ref=$CacheRef"
}

# Build
docker buildx build --platform $Platforms -t "$($ImageName):$($Tag)" $cacheOptions $pushFlag .

Write-Host "Done. Image $($ImageName):$($Tag) built for $Platforms"