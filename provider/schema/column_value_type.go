package schema

// ColumnType The type used to represent the value of the column, which is converted by the specific storage medium at the time of storage
type ColumnType int

const (

	// ColumnTypeNotAssign The default value is not set. If the value is not set, an error will be detected during initialization
	ColumnTypeNotAssign ColumnType = iota

	ColumnTypeSmallInt
	ColumnTypeInt
	ColumnTypeIntArray
	ColumnTypeBigInt

	ColumnTypeFloat

	ColumnTypeBool

	ColumnTypeString
	ColumnTypeStringArray

	ColumnTypeByteArray

	ColumnTypeTimestamp

	ColumnTypeJSON

	ColumnTypeIp
	ColumnTypeIpArray

	ColumnTypeCIDR
	ColumnTypeCIDRArray

	ColumnTypeMacAddr
	ColumnTypeMacAddrArray
)

func (x *ColumnType) String() string {
	switch *x {
	case ColumnTypeNotAssign:
		return "not_assign"
	case ColumnTypeSmallInt:
		return "small_int"
	case ColumnTypeInt:
		return "int"
	case ColumnTypeIntArray:
		return "int_array"
	case ColumnTypeBigInt:
		return "big_int"

	case ColumnTypeFloat:
		return "float"

	case ColumnTypeBool:
		return "bool"

	case ColumnTypeString:
		return "string"
	case ColumnTypeStringArray:
		return "string_array"

	case ColumnTypeByteArray:
		return "byte_array"

	case ColumnTypeTimestamp:
		return "timestamp"

	case ColumnTypeJSON:
		return "json"

	case ColumnTypeIp:
		return "ip"
	case ColumnTypeIpArray:
		return "ip_array"

	case ColumnTypeCIDR:
		return "cidr"
	case ColumnTypeCIDRArray:
		return "cidr_array"

	case ColumnTypeMacAddr:
		return "mac_address"
	case ColumnTypeMacAddrArray:
		return "mac_address_array"
	default:
		return "unknown"
	}
}
