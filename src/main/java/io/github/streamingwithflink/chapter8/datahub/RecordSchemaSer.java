package io.github.streamingwithflink.chapter8.datahub;

import com.aliyun.datahub.client.model.RecordSchema;

import java.io.Serializable;


/*
 * 为解决PutDatahubFunction 构造函数传入的参数必须序列化的问题，所以要 implements Serializable
 */

public class RecordSchemaSer
        extends RecordSchema
        implements Serializable
{


}
