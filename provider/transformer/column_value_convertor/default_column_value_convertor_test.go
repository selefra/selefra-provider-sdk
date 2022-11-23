package column_value_convertor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_Convert(t *testing.T) {
	diagnostics := schema.NewDiagnostics()
	clientMeta := schema.ClientMeta{}
	runtime, d := schema.NewClientMetaRuntime(context.Background(), "./", "test", &clientMeta, nil, true)
	assert.False(t, diagnostics.Add(d).HasError())
	_ = reflect_util.SetStructPtrUnExportedStrField(&clientMeta, "runtime", runtime)

	validString := []string{
		"",
		"N/A",
		"not_supported",
	}
	convertor := NewDefaultTypeConvertor(&clientMeta, validString)
	table := &schema.Table{
		TableName: "test",
	}
	column := &schema.Column{
		ColumnName: "foo",
		Type:       schema.ColumnTypeInt,
	}

	// test valid string
	for validString, _ := range convertor.validStringBlackSet {
		v, err := convertor.Convert(table, column, validString)
		assert.Nil(t, err)
		assert.Nil(t, v)
	}
	// test valid string type alias
	type FooString string
	var foo FooString = "N/A"
	column.Type = schema.ColumnTypeInt
	v, d := convertor.Convert(table, column, foo)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.Nil(t, v)
	column.Type = schema.ColumnTypeString
	v, d = convertor.Convert(table, column, foo)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.Equal(t, v, "N/A")

	// convert string to int
	column.Type = schema.ColumnTypeInt
	v, d = convertor.Convert(table, column, "1")
	assert.False(t, diagnostics.Add(d).HasError())
	assert.Equal(t, int(1), int(v.(int)))

	// convert bad time string to time.Time
	column.Type = schema.ColumnTypeTimestamp
	v, d = convertor.Convert(table, column, "i am bad time string")
	assert.True(t, diagnostics.Add(d).HasError())
	assert.Nil(t, v)

}

func Test_convertToSmallInt(t *testing.T) {

	var vxpectValue int16 = 10

	// int
	var a1 int = int(vxpectValue)
	v, err := convertToSmallInt(a1)
	assert.Nil(t, err)
	assert.Equal(t, v, vxpectValue)

	// *int
	var a2 *int = &a1
	v, err = convertToSmallInt(a2)
	assert.Nil(t, err)
	assert.Equal(t, v, vxpectValue)

	// **int
	var a3 **int = &a2
	v, err = convertToSmallInt(a3)
	assert.Nil(t, err)
	assert.Equal(t, v, vxpectValue)

	// string
	// *string
	// **string

	// float
	// *float
	// **float

	// type int
	// *type int
	// **type int

}

func Test_convertToJson(t *testing.T) {

	jsonString := `{"Bar":"bar"}`

	// []byte
	byteArray := []byte(jsonString)
	v, err := convertToJson(byteArray)
	assert.Nil(t, err)
	assert.Equal(t, v, jsonString)

	// *[]byte
	v, err = convertToJson(&byteArray)
	assert.Nil(t, err)
	assert.Equal(t, jsonString, v)

	// []*byte
	bytePointerArray := make([]*byte, len(byteArray))
	for index, _ := range byteArray {
		bytePointerArray[index] = &byteArray[index]
	}
	v, err = convertToJson(bytePointerArray)
	assert.Nil(t, err)
	assert.Equal(t, jsonString, v)

	// *[]*byte
	v, err = convertToJson(&bytePointerArray)
	assert.Nil(t, err)
	assert.Equal(t, jsonString, v)

	// struct
	type Foo struct {
		Bar string
	}
	foo := Foo{
		Bar: "bar",
	}
	v, err = convertToJson(foo)
	assert.Nil(t, err)
	assert.Equal(t, v, `{"Bar":"bar"}`)

	// *struct
	v, err = convertToJson(&foo)
	assert.Nil(t, err)
	assert.Equal(t, v, `{"Bar":"bar"}`)

	// string
	v, err = convertToJson(jsonString)
	assert.Nil(t, err)
	assert.Equal(t, v, jsonString)

	// *string
	v, err = convertToJson(&jsonString)
	assert.Nil(t, err)
	assert.Equal(t, v, jsonString)

	// nil
	v, err = convertToJson(nil)
	assert.Nil(t, err)
	assert.Nil(t, v)

}

func Test_convertToTimestamp(t *testing.T) {

	s := "2022-09-02 18:39:29"
	testTime, _ := time.Parse("2006-01-02 15:04:05", s)
	timestamp := 1662115169

	// nil
	v, err := ConvertToTimestamp(nil)
	assert.Nil(t, err)
	assert.Nil(t, v)

	// string
	v, err = ConvertToTimestamp(s)
	assert.Nil(t, err)
	assert.False(t, reflect_util.IsNil(v))
	assert.Equal(t, s, v.Format("2006-01-02 15:04:05"))

	// *string
	v, err = ConvertToTimestamp(&s)
	assert.Nil(t, err)
	assert.False(t, reflect_util.IsNil(v))
	assert.Equal(t, s, v.Format("2006-01-02 15:04:05"))

	// time.Time
	v, err = ConvertToTimestamp(testTime)
	assert.Nil(t, err)
	assert.False(t, reflect_util.IsNil(v))
	assert.Equal(t, s, v.Format("2006-01-02 15:04:05"))

	// *time.Time
	v, err = ConvertToTimestamp(&testTime)
	assert.Nil(t, err)
	assert.False(t, reflect_util.IsNil(v))
	assert.Equal(t, s, v.Format("2006-01-02 15:04:05"))

	// uint64 unix timestamp
	v, err = ConvertToTimestamp(timestamp)
	assert.Nil(t, err)
	assert.Equal(t, s, v.Format("2006-01-02 15:04:05"))

	// uint64 unix timestamp pointer
	v, err = ConvertToTimestamp(1662115169)
	assert.Nil(t, err)
	assert.Equal(t, s, v.Format("2006-01-02 15:04:05"))

	v, err = ConvertToTimestamp("2022-10-24T08:01Z")
	assert.Nil(t, err)
	assert.Equal(t, "2022-10-24 08:01:00", v.Format("2006-01-02 15:04:05"))

	v, err = ConvertToTimestamp("")
	assert.Nil(t, err)
	assert.Nil(t, v)

}

func Test_convertToString(t *testing.T) {

	type Foo string
	var foo Foo = "foo value"
	s, err := convertToString(&foo)
	assert.Nil(t, err)
	assert.Equal(t, s, "foo value")

}

func Test_convertToBool(t *testing.T) {

	b := true

	// nil
	v, err := convertToBool(nil)
	assert.Nil(t, err)
	assert.Nil(t, v)

	// bool
	v, err = convertToBool(b)
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.True(t, v.(bool))

	// *bool
	v, err = convertToBool(&b)
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.True(t, v.(bool))

	// int
	n := 1
	v, err = convertToBool(n)
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.True(t, v.(bool))

	// *int
	v, err = convertToBool(&n)
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.True(t, v.(bool))

	// string
	s := "true"
	v, err = convertToBool(s)
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.True(t, v.(bool))

	// *string
	v, err = convertToBool(*&s)
	assert.Nil(t, err)
	assert.NotNil(t, v)
	assert.True(t, v.(bool))

	v, err = convertToBool("")
	assert.Nil(t, err)
	assert.Nil(t, v)

}

func Test_convertToStringSlice(t *testing.T) {

	type Foo string
	slice := make([]Foo, 0)
	slice = append(slice, "a")
	slice = append(slice, "b")
	stringSlice, err := convertToStringSlice(slice)
	assert.Nil(t, err)
	t.Log(stringSlice)

}

func Test_convertToJsonString(t *testing.T) {
	s := "foobar"
	jsonString, err := convertToJsonString(s)
	assert.Nil(t, err)
	assert.Equal(t, "[\"foobar\"]", jsonString)
}
