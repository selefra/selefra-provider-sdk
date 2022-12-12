package provider

import (
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

	err = getter.Get(providerDownloadDirectory, choosedFile.DownloadUrl)
	if err != nil {
		return "", err
	}

	executable, err := x.findExecutable(providerDownloadDirectory)
	if err != nil {
		return "", err
	}

	return executable, nil
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
	ProviderName    string
	ProviderVersion string
	DownloadUrl     string
	Sha256Sum       string
	Arch            string
	OS              string
}
