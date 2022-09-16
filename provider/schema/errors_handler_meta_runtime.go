package schema

type ErrorsHandlerMetaRuntime struct {
	myErrorsHandlerMeta *ErrorsHandlerMeta

	isNeedIgnoredErrorsSet map[IgnoredError]struct{}
	isNeedIgnoredAllError  bool
}

func NewErrorsHandlerMetaRuntime(myErrorsHandlerMeta *ErrorsHandlerMeta) *ErrorsHandlerMetaRuntime {

	// isNeedIgnoredErrorsSet
	isNeedIgnoredAllError := false
	isNeedIgnoredErrorsSet := make(map[IgnoredError]struct{}, 0)
	for _, err := range myErrorsHandlerMeta.IgnoredErrors {
		if err == IgnoredErrorAll {
			isNeedIgnoredAllError = true
		}
		isNeedIgnoredErrorsSet[err] = struct{}{}
	}

	return &ErrorsHandlerMetaRuntime{
		myErrorsHandlerMeta:    myErrorsHandlerMeta,
		isNeedIgnoredErrorsSet: isNeedIgnoredErrorsSet,
		isNeedIgnoredAllError:  isNeedIgnoredAllError,
	}
}

func (x *ErrorsHandlerMetaRuntime) IsNeedIgnore(err IgnoredError) bool {

	if x.isNeedIgnoredAllError {
		return true
	}

	_, exists := x.isNeedIgnoredErrorsSet[err]
	return exists
}
