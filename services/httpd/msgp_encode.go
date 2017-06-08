package httpd

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Message) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zxvk uint32
	zxvk, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zxvk > 0 {
		zxvk--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "level":
			z.Level, err = dc.ReadString()
			if err != nil {
				return
			}
		case "text":
			z.Text, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Message) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "level"
	err = en.Append(0x82, 0xa5, 0x6c, 0x65, 0x76, 0x65, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Level)
	if err != nil {
		return
	}
	// write "text"
	err = en.Append(0xa4, 0x74, 0x65, 0x78, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Text)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Message) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Level) + 5 + msgp.StringPrefixSize + len(z.Text)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *ResultHeader) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zbai uint32
	zbai, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zbai > 0 {
		zbai--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "id":
			z.ID, err = dc.ReadInt()
			if err != nil {
				return
			}
		case "messages":
			var zcmr uint32
			zcmr, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Messages) >= int(zcmr) {
				z.Messages = (z.Messages)[:zcmr]
			} else {
				z.Messages = make([]Message, zcmr)
			}
			for zbzg := range z.Messages {
				var zajw uint32
				zajw, err = dc.ReadMapHeader()
				if err != nil {
					return
				}
				for zajw > 0 {
					zajw--
					field, err = dc.ReadMapKeyPtr()
					if err != nil {
						return
					}
					switch msgp.UnsafeString(field) {
					case "level":
						z.Messages[zbzg].Level, err = dc.ReadString()
						if err != nil {
							return
						}
					case "text":
						z.Messages[zbzg].Text, err = dc.ReadString()
						if err != nil {
							return
						}
					default:
						err = dc.Skip()
						if err != nil {
							return
						}
					}
				}
			}
		case "error":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					return
				}
				z.Error = nil
			} else {
				if z.Error == nil {
					z.Error = new(string)
				}
				*z.Error, err = dc.ReadString()
				if err != nil {
					return
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *ResultHeader) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "id"
	err = en.Append(0x83, 0xa2, 0x69, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteInt(z.ID)
	if err != nil {
		return
	}
	// write "messages"
	err = en.Append(0xa8, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Messages)))
	if err != nil {
		return
	}
	for zbzg := range z.Messages {
		// map header, size 2
		// write "level"
		err = en.Append(0x82, 0xa5, 0x6c, 0x65, 0x76, 0x65, 0x6c)
		if err != nil {
			return err
		}
		err = en.WriteString(z.Messages[zbzg].Level)
		if err != nil {
			return
		}
		// write "text"
		err = en.Append(0xa4, 0x74, 0x65, 0x78, 0x74)
		if err != nil {
			return err
		}
		err = en.WriteString(z.Messages[zbzg].Text)
		if err != nil {
			return
		}
	}
	// write "error"
	err = en.Append(0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return err
	}
	if z.Error == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = en.WriteString(*z.Error)
		if err != nil {
			return
		}
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *ResultHeader) Msgsize() (s int) {
	s = 1 + 3 + msgp.IntSize + 9 + msgp.ArrayHeaderSize
	for zbzg := range z.Messages {
		s += 1 + 6 + msgp.StringPrefixSize + len(z.Messages[zbzg].Level) + 5 + msgp.StringPrefixSize + len(z.Messages[zbzg].Text)
	}
	s += 6
	if z.Error == nil {
		s += msgp.NilSize
	} else {
		s += msgp.StringPrefixSize + len(*z.Error)
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Row) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zhct uint32
	zhct, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zhct > 0 {
		zhct--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "values":
			var zcua uint32
			zcua, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Value) >= int(zcua) {
				z.Value = (z.Value)[:zcua]
			} else {
				z.Value = make([]interface{}, zcua)
			}
			for zwht := range z.Value {
				z.Value[zwht], err = dc.ReadIntf()
				if err != nil {
					return
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Row) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "values"
	err = en.Append(0x81, 0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Value)))
	if err != nil {
		return
	}
	for zwht := range z.Value {
		err = en.WriteIntf(z.Value[zwht])
		if err != nil {
			return
		}
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Row) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for zwht := range z.Value {
		s += msgp.GuessSize(z.Value[zwht])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *RowError) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zxhx uint32
	zxhx, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zxhx > 0 {
		zxhx--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "error":
			z.Error, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z RowError) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "error"
	err = en.Append(0x81, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Error)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z RowError) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Error)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesError) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zlqf uint32
	zlqf, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zlqf > 0 {
		zlqf--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "error":
			z.Error, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z SeriesError) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "error"
	err = en.Append(0x81, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Error)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z SeriesError) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Error)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesHeader) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zcxo uint32
	zcxo, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zcxo > 0 {
		zcxo--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "name":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					return
				}
				z.Name = nil
			} else {
				if z.Name == nil {
					z.Name = new(string)
				}
				*z.Name, err = dc.ReadString()
				if err != nil {
					return
				}
			}
		case "tags":
			var zeff uint32
			zeff, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Tags == nil && zeff > 0 {
				z.Tags = make(map[string]string, zeff)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zeff > 0 {
				zeff--
				var zdaf string
				var zpks string
				zdaf, err = dc.ReadString()
				if err != nil {
					return
				}
				zpks, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Tags[zdaf] = zpks
			}
		case "columns":
			var zrsw uint32
			zrsw, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Columns) >= int(zrsw) {
				z.Columns = (z.Columns)[:zrsw]
			} else {
				z.Columns = make([]string, zrsw)
			}
			for zjfb := range z.Columns {
				z.Columns[zjfb], err = dc.ReadString()
				if err != nil {
					return
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *SeriesHeader) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "name"
	err = en.Append(0x83, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	if z.Name == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = en.WriteString(*z.Name)
		if err != nil {
			return
		}
	}
	// write "tags"
	err = en.Append(0xa4, 0x74, 0x61, 0x67, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zdaf, zpks := range z.Tags {
		err = en.WriteString(zdaf)
		if err != nil {
			return
		}
		err = en.WriteString(zpks)
		if err != nil {
			return
		}
	}
	// write "columns"
	err = en.Append(0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Columns)))
	if err != nil {
		return
	}
	for zjfb := range z.Columns {
		err = en.WriteString(z.Columns[zjfb])
		if err != nil {
			return
		}
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *SeriesHeader) Msgsize() (s int) {
	s = 1 + 5
	if z.Name == nil {
		s += msgp.NilSize
	} else {
		s += msgp.StringPrefixSize + len(*z.Name)
	}
	s += 5 + msgp.MapHeaderSize
	if z.Tags != nil {
		for zdaf, zpks := range z.Tags {
			_ = zpks
			s += msgp.StringPrefixSize + len(zdaf) + msgp.StringPrefixSize + len(zpks)
		}
	}
	s += 8 + msgp.ArrayHeaderSize
	for zjfb := range z.Columns {
		s += msgp.StringPrefixSize + len(z.Columns[zjfb])
	}
	return
}
