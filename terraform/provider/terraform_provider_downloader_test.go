package provider

import (
	"github.com/stretchr/testify/assert"
	"runtime"
	"testing"
)

func TestProviderDownloader_Download(t *testing.T) {

	fileSlice := make([]*TerraformProviderFile, 0)
	fileSlice = append(fileSlice, &TerraformProviderFile{
		ProviderName:    "aws",
		ProviderVersion: "4.46.0",
		DownloadUrl:     "https://releases.hashicorp.com/terraform-provider-aws/4.46.0/terraform-provider-aws_4.46.0_windows_amd64.zip",
		Arch:            runtime.GOARCH,
		OS:              runtime.GOOS,
	})
	downloader := NewProviderDownloader(fileSlice)
	download, err := downloader.Download("./providers")
	assert.Nil(t, err)
	assert.NotEmpty(t, download)

}
