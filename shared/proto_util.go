package shared

import (
	"context"
	"encoding/json"
	"encoding/base64"
	"fmt"
	"gopkg.in/yaml.v2"
	"google.golang.org/protobuf/encoding/protojson"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/reflect/protoreflect"
	"os"
)

var (
	envelopeRegistry *EnvelopeConfig
)

type EnvelopeConfig struct {
	ConfigDir		  string					 	 `yaml:"configDir"`
	EnvelopeProtoPath string						 `yaml:"envelopeProtoPath"`
	EnvelopeMessage   protoreflect.Name				 `yaml:"envelopeMessage"`
	EnvelopeTypeField protoreflect.Name				 `yaml:"envelopeTypeField"`
	EnvelopeDataField string						 `yaml:"envelopeDataField"`
	MessageDescriptor protoreflect.MessageDescriptor `yaml:"-"`
	TypeMappings	  []*TypeMapping				 `yaml:"typeMappings"`
	typeMap			  map[string]*TypeMapping		 `yaml:"-"`
}

type TypeMapping struct {
	Type			  string						 `yaml:"type"`
	ProtoPath		 string						 	 `yaml:"protoPath"`
	MessageName	   protoreflect.Name				 `yaml:"messageName"`
	FileDescriptor	protoreflect.FileDescriptor		 `yaml:"-"`
}

func (m EnvelopeConfig) GetMappingForType(messageType string) (*TypeMapping, error) {

	if envelopeRegistry == nil {
		return nil, fmt.Errorf("envelope configuration not intialized")
	}

	tm := m.typeMap[messageType]
	if tm == nil {
		return nil, fmt.Errorf("type not found")
	}
	if tm.FileDescriptor != nil {
		return tm, nil
	}

	var err error

	tm.FileDescriptor, err = GetDescriptor(envelopeRegistry.ConfigDir, tm.ProtoPath)
	if err != nil {
		return nil, err
	}
	return tm, nil
}


func InitEnvelopeRegistry(configDir string) error {

	env := &EnvelopeConfig{}
	path := configDir + SEP +  "envelope.yaml"
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	err2 := yaml.Unmarshal(b, &env)
	if err2 != nil {
		return err2
	}
	env.typeMap = make(map[string]*TypeMapping)
	for _, v := range env.TypeMappings {
		env.typeMap[v.Type] = v
	}
	env.ConfigDir = configDir
	fd, err := GetDescriptor(configDir, env.EnvelopeProtoPath)
	if err != nil {
		return err
	}
	env.MessageDescriptor = fd.Messages().ByName(env.EnvelopeMessage)
	envelopeRegistry = env
	return nil
}


func Unmarshal(payload []byte, out *map[string]interface{}) error {

	// open envelope
	emd := envelopeRegistry.MessageDescriptor
	em := dynamicpb.NewMessage(emd)

	if err := proto.Unmarshal(payload, em); err != nil {
		return err
	}

	ejson, err := protojson.Marshal(em)
	if err != nil {
		return err
	}

	envelopeMap := make(map[string]interface{})

	if err = json.Unmarshal(ejson, &envelopeMap); err != nil {
		return err
	}

	// get type and data from envelope
	f_type := emd.Fields().ByName(envelopeRegistry.EnvelopeTypeField)
	m_type := em.Get(f_type)

	tm, err := envelopeRegistry.GetMappingForType(m_type.String())
	if tm == nil {
		return nil
	}
	if err != nil {
		return err
	}

	b64_data, err := GetPath(envelopeRegistry.EnvelopeDataField, envelopeMap, false, false)
	if err != nil {
		return fmt.Errorf("cant locate data in envelope for %s - %v", envelopeRegistry.EnvelopeDataField, err)
	}

	if b64_data == nil {
		return fmt.Errorf("cant locate data in envelope %v", envelopeMap)
	}

	msg_data, err := base64.StdEncoding.DecodeString(b64_data.(string))
	if err != nil {
		return err
	}

	fd := tm.FileDescriptor
	md := fd.Messages().ByName(tm.MessageName)

	m := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(msg_data, m); err != nil {
		return err
	}

	b, err := protojson.Marshal(m)
	if err != nil {
		return err
	}

	data := make(map[string]interface{})
	if err = json.Unmarshal(b, &data); err != nil {
		return err
	}

	// Merge the envelop and data maps
	mp := CreateNestedMapFromPath(envelopeRegistry.EnvelopeDataField, data)
	*out = MergeMaps(envelopeMap, mp)
	return nil
}

func GetDescriptor(configDir, protoPath string) (protoreflect.FileDescriptor, error) {

	comp := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: []string{configDir + SEP + "protobuf"},
		}),
	}
	ctx := context.Background()
	files, err := comp.Compile(ctx, protoPath)
	if err != nil {
		return nil, err
	}
	res, ok := files[0].(linker.Result)
	if !ok {
		return nil, fmt.Errorf("can't cast to linker.Result %v/%v", configDir, protoPath)
	}
	fds, ok2 := res.(linker.File)
	if !ok2 {
		return nil, fmt.Errorf("can't cast to linker.File %v/%v", configDir, protoPath)
	}
	fd, ok3 := fds.(protoreflect.FileDescriptor)
	if !ok3 {
		return nil, fmt.Errorf("can't cast to protoreflect.FileDescriptor %v/%v", configDir, protoPath)
	}
	return fd, nil
}
