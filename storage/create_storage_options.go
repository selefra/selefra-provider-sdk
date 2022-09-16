package storage

// CreateStorageOptions Different storage media may have different options that can be passed when creating a database
type CreateStorageOptions interface {
	ToJsonString() (string, error)
	FromJsonString(jsonString string) error
}
