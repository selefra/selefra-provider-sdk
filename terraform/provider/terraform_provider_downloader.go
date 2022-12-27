package provider

import (
	"fmt"
	"github.com/hashicorp/go-getter"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type TerraformProviderDownloader struct {
	files []*TerraformProviderFile
}

func NewProviderDownloader(files []*TerraformProviderFile) *TerraformProviderDownloader {
	return &TerraformProviderDownloader{
		files: files,
	}
}

func (x *TerraformProviderDownloader) Download(targetDirectory string) (string, error) {

	var choosedFile *TerraformProviderFile
	for _, file := range x.files {
		if runtime.GOARCH == file.Arch && runtime.GOOS == file.OS {
			choosedFile = file
			break
		}
	}
	if choosedFile == nil {
		return "", nil
	}

	providerDownloadDirectory := targetDirectory + "/" + choosedFile.ProviderName + "/" + choosedFile.ProviderVersion
	stat, err := os.Stat(targetDirectory)
	if err == nil && stat.IsDir() {
		executable, err := x.findExecutable(providerDownloadDirectory)
		if err == nil && executable != "" {
			return executable, nil
		}
	}

	var downloadError error
	for i := 0; i < 10; i++ {
		downloadError = getter.Get(providerDownloadDirectory, choosedFile.DownloadUrl)
		if downloadError != nil {
			continue
		}

		executable, err := x.findExecutable(providerDownloadDirectory)
		if err != nil {
			downloadError = err
			continue
		}

		return executable, nil
	}
	return "", fmt.Errorf("download failed: %s", downloadError.Error())
}

func (x *TerraformProviderDownloader) findExecutable(providerDownloadDirectory string) (string, error) {
	var executable string
	err := filepath.Walk(providerDownloadDirectory, func(path string, info fs.FileInfo, err error) error {
		if info != nil && strings.HasPrefix(info.Name(), "terraform-provider-") {
			executable = path
			return nil
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return executable, nil
}

type TerraformProviderFile struct {
	ProviderName    string `mapstructure:"provider-name"`
	ProviderVersion string `mapstructure:"provider-version"`
	DownloadUrl     string `mapstructure:"download-url"`
	Sha256Sum       string `mapstructure:"sha256-sum"`
	Arch            string `mapstructure:"arch"`
	OS              string `mapstructure:"os"`
}
