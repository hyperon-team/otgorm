package otgorm

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

//CreateSpanAttribute creates a KeyValue for use as a span attribute
func CreateSpanAttribute(k string, v interface{}) (kv attribute.KeyValue) {
	switch x := v.(type) {
	case string:
	case bool:
		return attribute.Bool(k, x)
	case int64:
		return attribute.Int64(k, x)
	case int32:
		return attribute.Int(k, int(x))
	case int:
		return attribute.Int(k, x)
	case float64:
		return attribute.Float64(k, x)
	case float32:
		return attribute.Float64(k, float64(x))
	case uint:
		return attribute.Int(k, int(x))
	case uint64:
		return attribute.Int64(k, int64(x))
	case uint32:
		return attribute.Int(k, int(x))
	}
	return attribute.String("attribute.error", fmt.Sprintf("couldn't convert %s into KeyValue", v))
}
