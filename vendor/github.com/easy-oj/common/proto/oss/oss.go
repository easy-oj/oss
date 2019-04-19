package oss

import "github.com/easy-oj/common/proto/base"

func NewGetObjectReq() *GetObjectReq {
	return &GetObjectReq{
		BaseReq: base.NewBaseReq(),
	}
}

func NewGetObjectResp() *GetObjectResp {
	return &GetObjectResp{
		BaseResp: base.NewBaseResp(),
	}
}

func NewPutObjectReq() *PutObjectReq {
	return &PutObjectReq{
		BaseReq: base.NewBaseReq(),
	}
}

func NewPutObjectResp() *PutObjectResp {
	return &PutObjectResp{
		BaseResp: base.NewBaseResp(),
	}
}
