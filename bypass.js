Java.perform(function() {
    var array_list = Java.use("java.util.ArrayList");
    var ApiClient = Java.use('com.android.org.conscrypt.TrustManagerImpl');
    
    ApiClient.verifyChainInternal.overload('javax.net.ssl.SSLSocket', '[Ljava.security.cert.X509Certificate;', 'java.lang.String').implementation = function(a, b, c) {
        return array_list.$new();
    };

    var X509TrustManager = Java.use('javax.net.ssl.X509TrustManager');
    var SSLContext = Java.use('javax.net.ssl.SSLContext');

    var TrustManager = Java.registerClass({
        name: 'dev.fran.TrustManager',
        implements: [X509TrustManager],
        methods: {
            checkClientTrusted: function(chain, authType) {},
            checkServerTrusted: function(chain, authType) {},
            getAcceptedIssuers: function() { return []; }
        }
    });

    var TrustManagers = [TrustManager.$new()];
    var SSLContext_init = SSLContext.init.overload('[Ljavax.net.ssl.KeyManager;', '[Ljavax.net.ssl.TrustManager;', 'java.security.SecureRandom');
    SSLContext_init.implementation = function(keyManager, trustManager, secureRandom) {
        SSLContext_init.call(this, keyManager, TrustManagers, secureRandom);
    };

    console.log("[+] SSL Pinning bypassed!");
});