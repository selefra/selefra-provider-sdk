package column_value_convertor

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/pointer"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/selefra/selefra-utils/pkg/runtime_util"
	"github.com/spf13/cast"
)

type DefaultColumnValueConvertor struct {
	clientMeta          *schema.ClientMeta
	validStringBlackSet map[string]struct{}
}

var _ schema.ColumnValueConvertor = &DefaultColumnValueConvertor{}

func NewDefaultTypeConvertor(clientMeta *schema.ClientMeta, validStringBlackSlice []string) *DefaultColumnValueConvertor {

	validStringBlackSet := make(map[string]struct{})
	for _, validString := range validStringBlackSlice {
		validStringBlackSet[validString] = struct{}{}
	}
	return &DefaultColumnValueConvertor{
		clientMeta:          clientMeta,
		validStringBlackSet: validStringBlackSet,
	}
}

// ------------------------------------------------- api valid string ------------------------------------------------------------------------

func (x *DefaultColumnValueConvertor) isValidString(column *schema.Column, columnValue any) bool {
	// do nothing if column type is string
	if column.Type == schema.ColumnTypeString {
		return false
	}
	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.String:
		if _, exists := x.validStringBlackSet[value.String()]; exists {
			return true
		} else {
			return false
		}
	case reflect.Pointer:
		return x.isValidString(column, value.Elem())
	}
	return false
}

// ------------------------------------------------- ------------------------------------------------------------------------

func (x *DefaultColumnValueConvertor) Convert(table *schema.Table, column *schema.Column, columnValue any) (newColumnValue any, diagnostics *schema.Diagnostics) {

	diagnostics = schema.NewDiagnostics()

	// must not nil
	if reflect_util.IsNil(columnValue) {
		return nil, nil
	}

	// must not valid string
	if x.isValidString(column, columnValue) {
		return nil, nil
	}

	var convertError error

	// try-catch panic
	defer func() {
		if r := recover(); r != nil {

			msg := strings.Builder{}
			msg.WriteString(fmt.Sprintf("table %s column %s convert panic: %s, unable to cast %#v of type %T to %s", table.TableName, column.ColumnName, r, columnValue, columnValue, column.Type.String()))
			diagnostics.AddErrorMsg(msg.String())
			newColumnValue = nil

			msg.WriteString("\nStack: \n")
			msg.WriteString(runtime_util.Stack())
			x.clientMeta.Error(msg.String())

		}
	}()

	switch column.Type {
	case schema.ColumnTypeSmallInt:
		newColumnValue, convertError = convertToSmallInt(columnValue)
	case schema.ColumnTypeInt:
		newColumnValue, convertError = convertToInt(columnValue)
	case schema.ColumnTypeIntArray:
		newColumnValue, convertError = convertToIntSlice(columnValue)
	case schema.ColumnTypeBigInt:
		newColumnValue, convertError = convertToBigInt(columnValue)

	case schema.ColumnTypeFloat:
		newColumnValue, convertError = convertToFloat(columnValue)

	case schema.ColumnTypeBool:
		newColumnValue, convertError = convertToBool(columnValue)

	case schema.ColumnTypeString:
		newColumnValue, convertError = convertToString(columnValue)
	case schema.ColumnTypeStringArray:
		newColumnValue, convertError = convertToStringSlice(columnValue)

	case schema.ColumnTypeByteArray:
		newColumnValue, convertError = convertToByteArray(columnValue)

	case schema.ColumnTypeTimestamp:
		newColumnValue, convertError = ConvertToTimestamp(columnValue)

	case schema.ColumnTypeJSON:
		newColumnValue, convertError = convertToJson(columnValue)

	case schema.ColumnTypeIp:
		newColumnValue, convertError = convertToIp(columnValue)
	case schema.ColumnTypeIpArray:
		newColumnValue, convertError = convertToIpArray(columnValue)

	case schema.ColumnTypeCIDR:
		newColumnValue, convertError = convertToCIDR(columnValue)
	case schema.ColumnTypeCIDRArray:
		newColumnValue, convertError = convertToCIDRArray(columnValue)

	case schema.ColumnTypeMacAddr:
		newColumnValue, convertError = convertToMacAddr(columnValue)
	case schema.ColumnTypeMacAddrArray:
		newColumnValue, convertError = convertToMacAddrArray(columnValue)
	}

	// If has error, drop value and return error
	if convertError != nil {
		return nil, diagnostics.AddErrorMsg("table %s column %s type convert error: %s", table.TableName, column.ColumnName, convertError.Error())
	}

	return newColumnValue, diagnostics
}

// ------------------------------------------------- SmallInt ----------------------------------------------------------

func convertToSmallInt(columnValue any) (any, error) {
	v := indirect(columnValue)

	intv, ok := toInt(v)
	if ok {
		return int16(intv), nil
	}

	switch s := v.(type) {
	case int64:
		return int16(s), nil
	case int32:
		return int16(s), nil
	case int16:
		return s, nil
	case int8:
		return int16(s), nil
	case uint:
		return int16(s), nil
	case uint64:
		return int16(s), nil
	case uint32:
		return int16(s), nil
	case uint16:
		return int16(s), nil
	case uint8:
		return int16(s), nil
	case float64:
		return int16(s), nil
	case float32:
		return int16(s), nil
	case string:
		//if _, exists := validStringBlackSet[s]; exists {
		//	return nil, nil
		//}
		v, err := strconv.ParseInt(trimZeroDecimal(s), 0, 0)
		if err == nil {
			return int16(v), nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to int16", v, v)
	case json.Number:
		return convertToSmallInt(string(s))
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	}

	reflectValue := reflect.ValueOf(columnValue)
	switch reflectValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int16(reflectValue.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int16(reflectValue.Uint()), nil
	case reflect.Pointer:
		if reflectValue.Elem().IsValid() {
			return convertToSmallInt(reflectValue.Elem().Interface())
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to int16", v, v)
}

// toInt returns the int value of v if v or v's underlying type
// is an int.
// Note that this will return false for int64 etc. types.
func toInt(v any) (int, bool) {
	switch v := v.(type) {
	case int:
		return v, true
	case time.Weekday:
		return int(v), true
	case time.Month:
		return int(v), true
	default:
		return 0, false
	}
}

// From html/template/content.go
// Copyright 2011 The Go Authors. All rights reserved.
// indirect returns the value, after dereferencing as many times
// as necessary to reach the base type (or nil).
func indirect(a any) any {
	if a == nil {
		return nil
	}
	if t := reflect.TypeOf(a); t.Kind() != reflect.Ptr {
		// Avoid creating a reflect.Value if it's not a pointer.
		return a
	}
	v := reflect.ValueOf(a)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

func trimZeroDecimal(s string) string {
	var foundZero bool
	for i := len(s); i > 0; i-- {
		switch s[i-1] {
		case '.':
			if foundZero {
				return s[:i-1]
			}
		case '0':
			foundZero = true
		default:
			return s
		}
	}
	return s
}

// ------------------------------------------------- Int ---------------------------------------------------------------

func convertToInt(columnValue any) (any, error) {
	v := indirect(columnValue)

	intv, ok := toInt(v)
	if ok {
		return intv, nil
	}

	switch s := v.(type) {
	case int64:
		return int(s), nil
	case int32:
		return int(s), nil
	case int16:
		return int(s), nil
	case int8:
		return int(s), nil
	case uint:
		return int(s), nil
	case uint64:
		return int(s), nil
	case uint32:
		return int(s), nil
	case uint16:
		return int(s), nil
	case uint8:
		return int(s), nil
	case float64:
		return int(s), nil
	case float32:
		return int(s), nil
	case string:
		//if _, exists := validStringBlackSet[s]; exists {
		//	return nil, nil
		//}
		v, err := strconv.ParseInt(trimZeroDecimal(s), 0, 0)
		if err == nil {
			return int(v), nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to int64", v, v)
	case json.Number:
		return convertToInt(string(s))
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(value.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(value.Uint()), nil
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return convertToInt(value.Elem().Interface())
		}
	}

	return 0, fmt.Errorf("unable to cast %#v of type %T to int", v, v)
}

func convertToIntSlice(columnValue any) (any, error) {
	switch v := columnValue.(type) {
	case []int:
		return v, nil
		//case *string:
		//	//if _, exists := validStringBlackSet[*v]; exists {
		//	//	return nil, nil
		//	//}
		//case string:
		//	if _, exists := validStringBlackSet[v]; exists {
		//		return nil, nil
		//	}
	}

	reflectValue := reflect.ValueOf(columnValue)
	switch reflectValue.Kind() {
	case reflect.Slice, reflect.Array:
		valueSlice := reflect.ValueOf(columnValue)
		intSlice := make([]int, valueSlice.Len())
		for i := 0; i < valueSlice.Len(); i++ {
			val, err := convertToInt(valueSlice.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			if val != nil {
				intSlice[i] = val.(int)
			}
		}
		return intSlice, nil
	case reflect.Pointer:
		if reflectValue.Elem().IsValid() {
			return convertToIntSlice(reflectValue.Elem().Interface())
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to []int", columnValue, columnValue)
}

// ------------------------------------------------- BigInt ------------------------------------------------------------

func convertToBigInt(columnValue any) (any, error) {
	v := indirect(columnValue)

	intv, ok := toInt(v)
	if ok {
		return int64(intv), nil
	}

	switch s := v.(type) {
	case int64:
		return s, nil
	case int32:
		return int64(s), nil
	case int16:
		return int64(s), nil
	case int8:
		return int64(s), nil
	case uint:
		return int64(s), nil
	case uint64:
		return int64(s), nil
	case uint32:
		return int64(s), nil
	case uint16:
		return int64(s), nil
	case uint8:
		return int64(s), nil
	case float64:
		return int64(s), nil
	case float32:
		return int64(s), nil
	case string:
		//if _, exists := validStringBlackSet[s]; exists {
		//	return nil, nil
		//}
		v, err := strconv.ParseInt(trimZeroDecimal(s), 0, 0)
		if err == nil {
			return v, nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to int64", v, v)
	case json.Number:
		return convertToBigInt(string(s))
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint(), nil
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return convertToBigInt(value.Elem().Interface())
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to int64", v, v)
}

// ------------------------------------------------- Float -------------------------------------------------------------

func convertToFloat(columnValue any) (any, error) {
	v := indirect(columnValue)

	intv, ok := toInt(v)
	if ok {
		return float64(intv), nil
	}

	switch s := v.(type) {
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case int64:
		return float64(s), nil
	case int32:
		return float64(s), nil
	case int16:
		return float64(s), nil
	case int8:
		return float64(s), nil
	case uint:
		return float64(s), nil
	case uint64:
		return float64(s), nil
	case uint32:
		return float64(s), nil
	case uint16:
		return float64(s), nil
	case uint8:
		return float64(s), nil
	case string:
		//if _, exists := validStringBlackSet[s]; exists {
		//	return nil, nil
		//}
		v, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return v, nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to float64", v, v)
	case json.Number:
		v, err := s.Float64()
		if err == nil {
			return v, nil
		}
		return nil, fmt.Errorf("unable to cast %#v of type %T to float64", v, v)
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	case nil:
		return nil, nil
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Float(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Float(), nil
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return convertToFloat(value.Elem().Interface())
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to float64", v, v)
}

// ------------------------------------------------- Bool --------------------------------------------------------------

func convertToBool(columnValue any) (any, error) {

	if columnValue == nil {
		return nil, nil
	}

	v := indirect(columnValue)

	switch b := v.(type) {
	case bool:
		return b, nil
	case nil:
		return false, nil
	case int:
		if v.(int) != 0 {
			return true, nil
		}
		return false, nil
	case string:
		//if _, exists := validStringBlackSet[b]; exists {
		//	return nil, nil
		//}
		return strconv.ParseBool(v.(string))
	case json.Number:
		v, err := convertToBigInt(b)
		if err == nil {
			return v != 0, nil
		}
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", v, v)
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return convertToBool(value.Elem().Interface())
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to bool", v, v)
}

// ------------------------------------------------- String ------------------------------------------------------------

func convertToString(columnValue any) (any, error) {
	v := indirectToStringerOrError(columnValue)
	switch s := v.(type) {
	case string:
		return s, nil
	case *string:
		if s == nil {
			return nil, nil
		}
		return *s, nil
	case bool:
		return strconv.FormatBool(s), nil
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(s), 'f', -1, 32), nil
	case int:
		return strconv.Itoa(s), nil
	case int64:
		return strconv.FormatInt(s, 10), nil
	case int32:
		return strconv.Itoa(int(s)), nil
	case int16:
		return strconv.FormatInt(int64(s), 10), nil
	case int8:
		return strconv.FormatInt(int64(s), 10), nil
	case uint:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint64:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(s), 10), nil
	case json.Number:
		return s.String(), nil
	case []byte:
		return string(s), nil
	case template.HTML:
		return string(s), nil
	case template.URL:
		return string(s), nil
	case template.JS:
		return string(s), nil
	case template.CSS:
		return string(s), nil
	case template.HTMLAttr:
		return string(s), nil
	case nil:
		return "", nil
	case fmt.Stringer:
		return s.String(), nil
	case error:
		return s.Error(), nil
	}

	reflectValue := reflect.ValueOf(columnValue)
	switch reflectValue.Kind() {
	case reflect.String:
		return reflectValue.String(), nil
	case reflect.Pointer:
		if reflectValue.Elem().IsValid() {
			return convertToString(reflectValue.Elem().Interface())
		}
	default:
		// try to convert anything to json string, but may be panic if recursion
		jsonString, err := convertToJsonString(columnValue)
		if err == nil {
			if jsonString == "" {
				return nil, nil
			} else {
				return jsonString, nil
			}
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to string", v, v)

}

// From html/template/content.go
// Copyright 2011 The Go Authors. All rights reserved.
// indirectToStringerOrError returns the value, after dereferencing as many times
// as necessary to reach the base type (or nil) or an implementation of fmt.Stringer
// or error,
func indirectToStringerOrError(a any) any {
	if a == nil {
		return nil
	}

	var errorType = reflect.TypeOf((*error)(nil)).Elem()
	var fmtStringerType = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()

	v := reflect.ValueOf(a)
	for !v.Type().Implements(fmtStringerType) && !v.Type().Implements(errorType) && v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

func convertToStringSlice(columnValue any) (any, error) {
	var resultSlice []string

	switch v := columnValue.(type) {
	case []any:
		for _, u := range v {
			s, err := convertToString(u)
			if err != nil {
				return nil, err
			}
			if s != nil {
				resultSlice = append(resultSlice, s.(string))
			}
		}
		return resultSlice, nil
	case []string:
		return v, nil
	case []int8:
		for _, u := range v {
			s, err := convertToString(u)
			if err != nil {
				return nil, err
			}
			if s != nil {
				resultSlice = append(resultSlice, s.(string))
			}
		}
		return resultSlice, nil
	case []int:
		for _, u := range v {
			s, err := convertToString(u)
			if err != nil {
				return nil, err
			}
			if s != nil {
				resultSlice = append(resultSlice, s.(string))
			}
		}
		return resultSlice, nil
	case []int32:
		for _, u := range v {
			s, err := convertToString(u)
			if err != nil {
				return nil, err
			}
			if s != nil {
				resultSlice = append(resultSlice, s.(string))
			}
		}
		return resultSlice, nil
	case []int64:
		for _, u := range v {
			s, err := convertToString(u)
			if err != nil {
				return nil, err
			}
			if s != nil {
				resultSlice = append(resultSlice, s.(string))
			}
		}
		return resultSlice, nil
	case []float32:
		for _, u := range v {
			s, err := convertToString(u)
			if err != nil {
				return nil, err
			}
			if s != nil {
				resultSlice = append(resultSlice, s.(string))
			}
		}
		return resultSlice, nil
	case []float64:
		for _, u := range v {
			s, err := convertToString(u)
			if err != nil {
				return nil, err
			}
			if s != nil {
				resultSlice = append(resultSlice, s.(string))
			}
		}
		return resultSlice, nil
	case string:
		//if _, exists := validStringBlackSet[v]; exists {
		//	return nil, nil
		//}
		return strings.Fields(v), nil
	case *string:
		//if _, exists := validStringBlackSet[*v]; exists {
		//	return nil, nil
		//}
		return strings.Fields(*v), nil
	case []error:
		for _, err := range columnValue.([]error) {
			resultSlice = append(resultSlice, err.Error())
		}
		return resultSlice, nil
		//case any:
		//	str, err := convertToString(v)
		//	if err != nil {
		//		return resultSlice, fmt.Errorf("unable to cast %#v of type %T to []string", columnValue, columnValue)
		//	}
		//	if str != nil {
		//		return []string{str.(string)}, nil
		//	}
		//	return nil, nil
	}

	reflectValue := reflect.ValueOf(columnValue)
	switch reflectValue.Kind() {
	case reflect.String:
		return []string{reflectValue.String()}, nil
	case reflect.Pointer:
		if reflectValue.Elem().IsValid() {
			return convertToStringSlice(reflectValue.Elem().Interface())
		}
	case reflect.Slice, reflect.Array:
		valueSlice := reflect.ValueOf(columnValue)
		stringSlice := make([]string, valueSlice.Len())
		for i := 0; i < valueSlice.Len(); i++ {
			val, err := convertToString(valueSlice.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			if val != nil {
				stringSlice[i] = val.(string)
			}
		}
		return stringSlice, nil
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to []string", columnValue, columnValue)
}

// ------------------------------------------------- ByteArray ------------------------------------------------------------------------

var errNegativeNotAllowed = errors.New("unable to cast negative value")

func convertToByte(columnValue any) (any, error) {
	v := indirect(columnValue)

	intv, ok := toInt(v)
	if ok {
		if intv < 0 {
			return 0, errNegativeNotAllowed
		}
		return uint8(intv), nil
	}

	switch s := v.(type) {
	case string:
		v, err := strconv.ParseInt(trimZeroDecimal(s), 0, 0)
		if err == nil {
			if v < 0 {
				return 0, errNegativeNotAllowed
			}
			return uint8(v), nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to uint8", v, v)
	case json.Number:
		return convertToByte(string(s))
	case int64:
		if s < 0 {
			return 0, errNegativeNotAllowed
		}
		return uint8(s), nil
	case int32:
		if s < 0 {
			return 0, errNegativeNotAllowed
		}
		return uint8(s), nil
	case int16:
		if s < 0 {
			return 0, errNegativeNotAllowed
		}
		return uint8(s), nil
	case int8:
		if s < 0 {
			return 0, errNegativeNotAllowed
		}
		return uint8(s), nil
	case uint:
		return uint8(s), nil
	case uint64:
		return uint8(s), nil
	case uint32:
		return uint8(s), nil
	case uint16:
		return uint8(s), nil
	case uint8:
		return s, nil
	case float64:
		if s < 0 {
			return 0, errNegativeNotAllowed
		}
		return uint8(s), nil
	case float32:
		if s < 0 {
			return 0, errNegativeNotAllowed
		}
		return uint8(s), nil
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return convertToByte(value.Elem())
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to uint8", v, v)
}

func convertToByteArray(columnValue any) (any, error) {
	switch v := columnValue.(type) {
	case []byte:
		return v, nil
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Slice, reflect.Array:
		valueSlice := reflect.ValueOf(columnValue)
		bytes := make([]byte, valueSlice.Len())
		for i := 0; i < valueSlice.Len(); i++ {
			val, err := convertToInt(valueSlice.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			if val != nil {
				bytes[i] = val.(byte)
			}
		}
		return bytes, nil
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return convertToByteArray(value.Elem().Interface())
		}
	}
	return nil, fmt.Errorf("unable to cast %#v of type %T to []byte", columnValue, columnValue)
}

// ------------------------------------------------- TimeStamp ------------------------------------------------------------------------

type TimeFormatType int

const (
	TimeFormatNoTimezone TimeFormatType = iota
	TimeFormatNamedTimezone
	TimeFormatNumericTimezone
	TimeFormatNumericAndNamedTimezone
	TimeFormatTimeOnly
)

type TimeFormat struct {
	Formatter      string
	TimeFormatType TimeFormatType
}

var (
	timeFormats = []TimeFormat{
		{time.RFC3339, TimeFormatNumericTimezone},
		{"2006-01-02T15:04:05", TimeFormatNoTimezone}, // iso8601 without timezone
		{time.RFC1123Z, TimeFormatNumericTimezone},
		{time.RFC1123, TimeFormatNamedTimezone},
		{time.RFC822Z, TimeFormatNumericTimezone},
		{time.RFC822, TimeFormatNamedTimezone},
		{time.RFC850, TimeFormatNamedTimezone},
		{"2006-01-02 15:04:05.999999999 -0700 MST", TimeFormatNumericAndNamedTimezone}, // Time.String()
		{"2006-01-02T15:04:05-0700", TimeFormatNumericTimezone},                        // RFC3339 without timezone hh:mm colon
		{"2006-01-02 15:04:05Z0700", TimeFormatNumericTimezone},                        // RFC3339 without T or timezone hh:mm colon
		{"2006-01-02 15:04:05", TimeFormatNoTimezone},
		{time.ANSIC, TimeFormatNoTimezone},
		{time.UnixDate, TimeFormatNamedTimezone},
		{time.RubyDate, TimeFormatNumericTimezone},
		{"2006-01-02 15:04:05Z07:00", TimeFormatNumericTimezone},
		{"2006-01-02", TimeFormatNoTimezone},
		{"02 Jan 2006", TimeFormatNoTimezone},
		{"2006-01-02 15:04:05 -07:00", TimeFormatNumericTimezone},
		{"2006-01-02 15:04:05 -0700", TimeFormatNumericTimezone},
		{time.Kitchen, TimeFormatTimeOnly},
		{time.Stamp, TimeFormatTimeOnly},
		{time.StampMilli, TimeFormatTimeOnly},
		{time.StampMicro, TimeFormatTimeOnly},
		{time.StampNano, TimeFormatTimeOnly},
	}
)

func ConvertToTimestamp(columnValue any, formats ...TimeFormat) (*time.Time, error) {

	if columnValue == nil {
		return nil, nil
	}

	v := indirect(columnValue)

	switch v := v.(type) {
	case time.Time:
		if v.IsZero() {
			return nil, nil
		}
		return &v, nil
	case string:
		//if _, exists := validStringBlackSet[v]; exists {
		//	return nil, nil
		//}
		return parseDateWith(v, time.Local, timeFormats...)
	case json.Number:
		s, err1 := convertToBigInt(v)
		if err1 != nil {
			return nil, fmt.Errorf("unable to cast %#v of type %T to Time", v, v)
		}
		if s != nil && s != 0 {
			t := time.Unix(s.(int64), 0)
			return &t, nil
		}
	case int:
		if v == 0 {
			return nil, nil
		}
		return pointer.ToTimePointer(time.Unix(int64(v), 0)), nil
	case int64:
		if v == 0 {
			return nil, nil
		}
		return pointer.ToTimePointer(time.Unix(v, 0)), nil
	case int32:
		if v == 0 {
			return nil, nil
		}
		return pointer.ToTimePointer(time.Unix(int64(v), 0)), nil
	case uint:
		if v == 0 {
			return nil, nil
		}
		return pointer.ToTimePointer(time.Unix(int64(v), 0)), nil
	case uint64:
		if v == 0 {
			return nil, nil
		}
		return pointer.ToTimePointer(time.Unix(int64(v), 0)), nil
	case uint32:
		if v == 0 {
			return nil, nil
		}
		return pointer.ToTimePointer(time.Unix(int64(v), 0)), nil
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return ConvertToTimestamp(value.Elem().Interface())
		}
	}

	return nil, fmt.Errorf("unable to cast %#v of type %T to Time", v, v)
}

func parseDateWith(s string, location *time.Location, formats ...TimeFormat) (*time.Time, error) {

	for _, format := range formats {
		d, e := time.Parse(format.Formatter, s)
		if e == nil {

			// Some time formats have a zone name, but no offset, so it gets
			// put in that zone name (not the default one passed in to us), but
			// without that zone's offset. So set the location manually.
			if format.TimeFormatType <= TimeFormatNamedTimezone {
				if location == nil {
					location = time.Local
				}
				year, month, day := d.Date()
				hour, min, sec := d.Clock()
				d = time.Date(year, month, day, hour, min, sec, d.Nanosecond(), location)
			}
			return &d, e
		}
	}

	return nil, fmt.Errorf("unable to parse date: %s", s)
}

// ------------------------------------------------- MacAddr -----------------------------------------------------------

func convertToMacAddr(columnValue any) (any, error) {
	macString, err := cast.ToStringE(columnValue)
	if err != nil {
		return nil, err
	}
	return net.ParseMAC(macString)
}

func convertToMacAddrArray(columnValue any) (any, error) {
	macStringSlice, err := cast.ToStringSliceE(columnValue)
	if err != nil {
		return nil, err
	}
	macSlice := make([]net.HardwareAddr, len(macStringSlice))
	for i, macString := range macStringSlice {
		macSlice[i], err = net.ParseMAC(macString)
		if err != nil {
			return nil, err
		}
	}
	return macSlice, nil
}

// ------------------------------------------------- CIDR --------------------------------------------------------------

func convertToCIDR(columnValue any) (any, error) {
	if columnValue == nil {
		return nil, nil
	}
	switch v := columnValue.(type) {
	case *net.IPNet:
		return v, nil
	case net.IPNet:
		return &v, nil
	}

	sip, err := cast.ToStringE(columnValue)
	if err != nil {
		return nil, err
	}

	_, newColumnValue, err := net.ParseCIDR(sip)
	return newColumnValue, err
}

func convertToCIDRArray(columnValue any) (any, error) {
	if columnValue == nil {
		return nil, nil
	}
	switch v := columnValue.(type) {
	case []*net.IPNet:
		return v, nil
	case *[]*net.IPNet:
		return *v, nil
	case []net.IPNet:
		res := make([]*net.IPNet, len(v))
		for i := range res {
			res[i] = &v[i]
		}
		return res, nil

	case *[]net.IPNet:
		res := make([]*net.IPNet, len(*v))
		for i := range res {
			res[i] = &(*v)[i]
		}
		return res, nil
	case *net.IPNet:
		return []*net.IPNet{v}, nil
	case net.IPNet:
		return []*net.IPNet{&v}, nil
	}

	sips, err := cast.ToStringSliceE(columnValue)
	if err != nil {
		return nil, err
	}
	ips := make([]*net.IPNet, len(sips))
	for i, sip := range sips {
		_, ips[i], err = net.ParseCIDR(sip)
		if err != nil {
			return nil, err
		}
	}
	return ips, nil
}

// ------------------------------------------------- IP ----------------------------------------------------------------

func convertToIp(columnValue any) (any, error) {

	if columnValue == nil {
		return nil, nil
	}

	switch v := columnValue.(type) {
	case net.IP:
		return v, nil
	case *net.IP:
		return *v, nil
	}

	ipString, err := cast.ToStringE(columnValue)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(ipString)
	if ip == nil {
		return columnValue, nil
	}
	if ip.To4() != nil {
		return ip.To4(), nil
	}
	return ip, nil
}

func convertToIpArray(columnValue any) (any, error) {
	if columnValue == nil {
		return nil, nil
	}
	switch v := columnValue.(type) {
	case []net.IP:
		return v, nil
	case *[]net.IP:
		return *v, nil
	case []*net.IP:
		res := make([]net.IP, len(v))
		for i := range res {
			if v[i] == nil {
				v[i] = &net.IP{}
			}
			res[i] = *v[i]
		}
		return res, nil
	case *[]*net.IP:
		res := make([]net.IP, len(*v))
		for i := range res {
			if (*v)[i] == nil {
				(*v)[i] = &net.IP{}
			}
			res[i] = *(*v)[i]
		}
		return res, nil
	}

	var sips []string
	var err error
	switch v := columnValue.(type) {
	case *[]string:
		sips, err = cast.ToStringSliceE(*v)
	default:
		sips, err = cast.ToStringSliceE(columnValue)
	}
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, len(sips))
	for i, sip := range sips {
		ip := net.ParseIP(sip)
		if sip != "" && ip == nil {
			return nil, err
		}
		if ip.To4() != nil {
			ip = ip.To4()
		}
		ips[i] = ip
	}
	return ips, nil
}

// ------------------------------------------------- JSON --------------------------------------------------------------

func convertToJson(columnValue any) (any, error) {
	if columnValue == nil {
		return nil, nil
	}
	jsonString, err := convertToJsonString(columnValue)
	if err == nil {
		if jsonString == "" {
			// trim to nil if string is empty
			return nil, nil
		} else {
			return jsonString, nil
		}
	}

	value := reflect.ValueOf(columnValue)
	switch value.Kind() {
	case reflect.Pointer:
		if value.Elem().IsValid() {
			return convertToJson(value.Elem().Interface())
		}
	}

	return nil, err
}

func convertToJsonString(columnValue any) (string, error) {
	switch v := columnValue.(type) {
	case string:
		return v, nil
	case *string:
		if columnValue == nil {
			return "", nil
		}
		return *v, nil
	case []byte:
		return string(v), nil
	case *[]byte:
		return string(*v), nil
	case []*byte:
		rt := make([]byte, len(v))
		for i := range rt {
			if v[i] == nil {
				v[i] = new(byte)
			}
			rt[i] = *v[i]
		}
		return string(rt), nil
	case *[]*byte:
		if v == nil {
			return "", nil
		}
		rt := make([]byte, len(*v))
		for i := range rt {
			if (*v)[i] == nil {
				(*v)[i] = new(byte)
			}
			rt[i] = *(*v)[i]
		}
		return string(rt), nil
	}

	marshal, err := json.Marshal(columnValue)
	if err == nil {
		return string(marshal), nil
	}

	return "", fmt.Errorf("unable to cast %#v of type %T to JSON", columnValue, columnValue)
}

// ---------------------------------------------------------------------------------------------------------------------
