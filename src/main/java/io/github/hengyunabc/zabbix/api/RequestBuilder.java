package io.github.hengyunabc.zabbix.api;

import java.util.concurrent.atomic.AtomicInteger;

public final class RequestBuilder {
    private static final AtomicInteger NEXT_ID = new AtomicInteger(1);

    Request request = new Request();

    private RequestBuilder() {

    }

    public static RequestBuilder newBuilder() {
        return new RequestBuilder();
    }

    public Request build() {
        if (request.getId() == null) {
            request.setId(NEXT_ID.getAndIncrement());
        }
        return request;
    }

    public RequestBuilder version(String version) {
        request.setJsonrpc(version);
        return this;
    }

    public RequestBuilder paramEntry(String key, Object value) {
        request.putParam(key, value);
        return this;
    }

    /**
     * Do not necessary to call this method.If don not set id, ZabbixApi will auto set request auth..
     *
     * @param auth auth
     * @return return
     */
    public RequestBuilder auth(String auth) {
        request.setAuth(auth);
        return this;
    }

    public RequestBuilder method(String method) {
        request.setMethod(method);
        return this;
    }

    /**
     * Do not necessary to call this method.If don not set id, RequestBuilder will auto generate.
     *
     * @param id id
     * @return return
     */
    public RequestBuilder id(Integer id) {
        request.setId(id);
        return this;
    }
}
