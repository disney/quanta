package shared

import (
	"github.com/hamba/avro/v2"
	"regexp"
)

// ToAvroSchema - Generate an AVRO schema from a table.
func ToAvroSchema(table *BasicTable) avro.Schema {

	reg, err := regexp.Compile("[^a-zA-Z0-9_]+")
	if err != nil {
		panic(err)
	}

	fields := make([]*avro.Field, 0)
	for _, v := range table.Attributes {
		if v.SourceName == "" {
			continue
		}
		name := reg.ReplaceAllString(v.SourceName, "")
		var field *avro.Field
		switch TypeFromString(v.Type) {
		case String:
			field, _ = avro.NewField(name, avro.NewPrimitiveSchema(avro.String, nil))
		case Integer:
			field, _ = avro.NewField(name, avro.NewPrimitiveSchema(avro.Long, nil))
		case Float:
			field, _ = avro.NewField(name, avro.NewPrimitiveSchema(avro.Double, nil))
		case Date:
			field, _ = avro.NewField(name, avro.NewPrimitiveSchema(avro.Long,
				avro.NewPrimitiveLogicalSchema(avro.Date)))
		case DateTime:
			field, _ = avro.NewField(name, avro.NewPrimitiveSchema(avro.Long, nil))
			//field, _ = avro.NewField(name, avro.NewPrimitiveSchema(avro.Long,
			//	avro.NewPrimitiveLogicalSchema(avro.TimestampMillis)), nil)
		case Boolean:
			field, _ = avro.NewField(name, avro.NewPrimitiveSchema(avro.Boolean, nil))
		default:
		}
		fields = append(fields, field)
	}

	rs, err := avro.NewRecordSchema(table.Name, "quanta", fields)
	if err != nil {
		panic(err)
	}
	return rs
}
