

[::Go back to Oozie Documentation Index::](index.html)

# Creating Custom Authentication

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Hadoop-Auth Authentication Interfaces and classes

1. `org.apache.hadoop.security.authentication.client.Authenticator:` Interface for client authentication mechanisms.

    The following authenticators are provided in hadoop-auth:

       * KerberosAuthenticator   : the authenticator implements the Kerberos SPNEGO authentication sequence.
       * PseudoAuthenticator     : the authenticator implementation provides an authentication equivalent to Hadoop's Simple
       authentication, it trusts the value of the 'user.name' Java System property.

2. `org.apache.hadoop.security.authentication.server.AuthenticationHandler:` Interface for server authentication mechanisms.

       * KerberosAuthenticationHandler   : the authenticator handler implements the Kerberos SPNEGO authentication mechanism for HTTP.
       * PseudoAuthenticationHandler     : the authenticator handler provides a pseudo authentication mechanism that accepts the user
       name specified as a query string parameter.

3. `org.apache.hadoop.security.authentication.server.AuthenticationFilter:` A servlet filter enables protecting web application
    resources with different authentication mechanisms provided by AuthenticationHandler. To enable the filter, web application
    resources file (ex. web.xml) needs to include a filter class derived from `AuthenticationFilter`.

    For more information have a look at the appropriate
    [Hadoop documentation](https://hadoop.apache.org/docs/r2.7.2/hadoop-auth/index.html).

## Provide Custom Authentication to Oozie Client

Apache Oozie contains a default class `org.apache.oozie.client.AuthOozieClient` to support Kerberos HTTP SPNEGO authentication,
pseudo/simple authentication and anonymous access for client connections.

To provide other authentication mechanisms, an Oozie client should extend from `AuthOozieClient` and provide the following
methods should be overridden by derived classes to provide custom authentication:

   * getAuthenticator()   : return corresponding Authenticator based on value specified by user at `auth` command option.
   * createConnection()   : create a singleton class at Authenticator to allow client set and get key-value configuration for
   authentication.

## Provide Custom Authentication to Oozie Server

To accept custom authentication in Oozie server, a filter extends from AuthenticationFilter must be provided. This filter
delegates to the configured authentication handler for authentication and once it obtains an `AuthenticationToken` from it, sets
a signed HTTP cookie with the token. If HTTP cookie is provided with different key name, its cookie value can be retrieved by
overriding `getToken()` method. Please note, only when `getToken()` return NULL, a custom authentication can be invoked and
processed in `AuthenticationFilter.doFilter()`.

The following method explains how to read it and return NULL token.

```
protected AuthenticationToken getToken(HttpServletRequest request) throws IOException, AuthenticationException {
        String tokenStr = null;
        Cookie[] cookies = request.getCookies();

        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName().equals(AuthenticatedURL.AUTH_COOKIE)) {
                    tokenStr = cookie.getValue();
                    LOG.info("Got 'hadoop.auth' cookie from request = " + tokenStr);
                    if (tokenStr != null && !tokenStr.trim().isEmpty()) {
                        AuthenticationToken retToken = super.getToken(request);
                        return retToken;
                    }
                } else if (cookie.getName().equals("NEWAUTH")) {
                    tokenStr = cookie.getValue();
                    // DO NOT return the token string so request can authenticated.
                }
            }
        }
        return null;
      }
```

[::Go back to Oozie Documentation Index::](index.html)


