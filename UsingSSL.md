# Using TLS with PEM certificate files

This guide covers connecting the client to a broker over TLS using the PEM files that `openssl` produces (`ca.crt`,
`client.crt`, `client.key`) instead of Java keystores. It also covers setting up Mosquitto for development and
turning on hostname verification. For the TLS options themselves, see [TLS in the README](README.md#tls).

- [The files and who needs them](#the-files-and-who-needs-them)
- [Creating the certificates](#creating-the-certificates)
- [Setting up Mosquitto](#setting-up-mosquitto), on Windows and [on Linux](#mosquitto-on-linux)
- [Connecting from the client](#connecting-from-the-client)
- [Hostname verification](#hostname-verification)
- [What changed from the original helper](#what-changed-from-the-original-helper)
- [Troubleshooting](#troubleshooting)

Everything below was checked with Mosquitto 2.0.18, OpenSSL 3.2, Java 8u432, Netty 4.1.137 and BouncyCastle 1.82.

## The files and who needs them

| File | What it is | Needed by |
|---|---|---|
| `ca.crt` | The certificate of your own certificate authority (CA). | The broker (to check client certificates) and every client (to check the broker). |
| `ca.key` | The CA's private key, used only to sign certificates. | Nobody at run time. Keep it private, and out of source control and JARs. |
| `server.crt`, `server.key` | The broker's certificate, signed by the CA, and its private key. The certificate must name the broker's host. | The broker. |
| `client.crt`, `client.key` | A client certificate, signed by the CA, and its private key. | Clients, only if the broker requires client certificates. |
| `*.csr`, `ca.srl` | Signing requests and the CA's serial number file. | Nobody; leftovers from signing. `ca.srl` is reused when signing more certificates. |

There are two levels of TLS:

- **Server authentication** (the usual case): the client checks that the broker's certificate was signed by a CA it
  trusts, and with hostname verification on, that it names the host it connected to. The client needs `ca.crt`.
- **Mutual TLS**: the broker also asks the client for a certificate signed by its CA. The client also needs
  `client.crt` and `client.key`.

## Creating the certificates

The development batch file (Windows) and
[`examples/ssl/create-certs.sh`](examples/ssl/create-certs.sh) (Linux, macOS, or Git Bash on Windows) create a CA,
a server certificate and a client certificate in `ssl` under the current directory. They take the broker host name
as their only argument (default `localhost`):

```bat
create-certs.bat localhost
create-certs.bat mybroker.lan
```

```sh
bash create-certs.sh mybroker.lan
```

Both do the same:

1. **CA**: a 2048-bit RSA key and a self-signed certificate, `CN=Mosquitto-Local-CA`, valid for 10 years, marked as
   a CA (`basicConstraints=critical,CA:TRUE`, `keyUsage=keyCertSign,cRLSign`).
2. **Server certificate**: `CN=<host>` with `subjectAltName=DNS:<host>` and `extendedKeyUsage=serverAuth`, signed
   by the CA, valid for 825 days. The subject alternative name (SAN) is what hostname verification checks.
3. **Client certificate**: `CN=Mosquitto-Client` with `extendedKeyUsage=clientAuth`, signed by the CA.
4. **Checks**: `openssl verify -CAfile ca.crt -verify_hostname <host> server.crt` confirms that the server
   certificate chains to the CA and names the host. A client with hostname verification on makes the same check.

Things to know:

- **It needs OpenSSL 3.0 or later** on the `PATH`. The `-copy_extensions copy` option, which carries the SAN and key
  usages from the request into the certificate, was added in 3.0. Git for Windows includes OpenSSL 3 in
  `C:\Program Files\Git\mingw64\bin`, if no other `openssl` is installed.
- **OpenSSL 3 writes keys as PKCS#8** (`-----BEGIN PRIVATE KEY-----`). OpenSSL 1.x wrote the "traditional" format
  (`-----BEGIN RSA PRIVATE KEY-----`). Both client options below read either, but this matters for which one you
  choose.
- **The certificate must name every host clients use.** It names only `<host>`. A client that connects to
  `127.0.0.1`, or to the machine's other names, fails hostname verification (see
  [Hostname verification](#hostname-verification)). To allow several names, list them all in the server request's
  `subjectAltName`, using `DNS:` for names and `IP:` for addresses:

  ```bat
  -addext "subjectAltName=DNS:%BROKER_HOST%,DNS:localhost,IP:127.0.0.1"
  ```

- **Re-run it** when the host name changes or the certificates near expiry, then copy the new files to the broker and
  the clients. Re-running creates a new CA too, so every client then needs the new `ca.crt`.
- **The shell script's keys are as readable as your `umask` makes them** (usually `644`, readable by everyone). Run it
  with a private `umask`, so the keys are `600` from the start:

  ```sh
  (umask 077; bash create-certs.sh mybroker.lan)
  ```

  In Git Bash on Windows, prefix it with `MSYS_NO_PATHCONV=1`: otherwise Git Bash turns `-subj "/CN=..."` into a
  Windows path and OpenSSL rejects the subject.
- **The configuration the script prints uses `./ssl` paths.** Mosquitto resolves relative paths against its own
  working directory, not the configuration file's, so use absolute paths, as in
  [Mosquitto on Linux](#mosquitto-on-linux).

## Setting up Mosquitto

### Installing and configuring on Windows

On Windows, the installer puts Mosquitto in `C:\Program Files\mosquitto`, registers a service called **Mosquitto
Broker** (service name `mosquitto`), and the service reads `C:\Program Files\mosquitto\mosquitto.conf`. Copy
`ca.crt`, `server.crt` and `server.key` to `C:\Program Files\mosquitto\ssl` (the broker never needs `ca.key`).

This development configuration opens plain MQTT, plain WebSockets and a TLS listener for each:

```conf
# Plain MQTT
listener 1883
protocol mqtt
allow_anonymous true

# Plain WebSockets
listener 8083
protocol websockets

# WebSockets over TLS (wss://host:8084)
listener 8084
protocol websockets
cafile c:\Program Files\mosquitto\ssl\ca.crt
certfile c:\Program Files\mosquitto\ssl\server.crt
keyfile c:\Program Files\mosquitto\ssl\server.key
tls_version tlsv1.2

# MQTT over TLS (ssl://host:8883)
listener 8883
cafile c:\Program Files\mosquitto\ssl\ca.crt
certfile c:\Program Files\mosquitto\ssl\server.crt
keyfile c:\Program Files\mosquitto\ssl\server.key
tls_version tlsv1.2
```

Notes:

- **`allow_anonymous` applies to every listener**, wherever it appears, unless `per_listener_settings true` is set.
  Mosquitto 2.x refuses unauthenticated clients by default, so without the line every connection would need a user
  name and password. Repeating it on each listener does no harm. It is fine for development and not for anything
  reachable from other machines.
- **A listener without `protocol` is MQTT**, so the 8883 listener needs no `protocol mqtt` line.
- **`tls_version tlsv1.2` is the minimum version**, so TLS 1.3 is still allowed. The clients below ask for TLS 1.2.
- **Paths may contain spaces.** Everything after the option name is the path, and Windows paths need no quoting or
  escaping.
- **`cafile`** is the CA the broker uses to check client certificates; `certfile` and `keyfile` are what it presents
  to clients.

### Requiring client certificates

To accept only clients with a certificate signed by your CA, add to the TLS listener:

```conf
require_certificate true
use_identity_as_username true
```

- `require_certificate true` makes the broker reject a handshake without a valid client certificate. The client sees
  `Channel closed before CONNACK received`, and the broker logs `peer did not return a certificate`.
- `use_identity_as_username true` makes the certificate's CN the client's user name (`Mosquitto-Client` here), as if
  it had sent that user name, so an ACL file can grant topics per certificate. Without it, the certificate only gets
  the client in.

A second listener on another port can require certificates while the first does not; TLS options are always
per listener.

### Starting and checking the broker

After changing the configuration, restart the service (from an administrator prompt, or in `services.msc`):

```bat
net stop mosquitto
net start mosquitto
```

To see what the broker is doing, stop the service and run it in a console instead, with verbose logging:

```bat
"C:\Program Files\mosquitto\mosquitto.exe" -c "C:\Program Files\mosquitto\mosquitto.conf" -v
```

To check the TLS listeners, use Mosquitto's own clients, which verify the host name by default:

```bat
mosquitto_sub -h localhost -p 8883 --cafile ssl\ca.crt -t test/# -v
mosquitto_pub -h localhost -p 8883 --cafile ssl\ca.crt -t test/hello -m hi

REM with client certificates required
mosquitto_pub -h localhost -p 8883 --cafile ssl\ca.crt --cert ssl\client.crt --key ssl\client.key -t test/hello -m hi
```

Or look at the certificate the broker presents with `openssl s_client`:

```bat
openssl s_client -connect localhost:8883 -CAfile ssl\ca.crt -verify_hostname localhost -brief
```

### Mosquitto on Linux

These steps are for the Debian and Ubuntu `mosquitto` package, and were not tried on Linux here. Other distributions
differ mainly in the package name and the user the broker runs as. (The snap package is confined to
`/var/snap/mosquitto/common`, so keep its files there instead.)

```sh
sudo apt install mosquitto mosquitto-clients
```

The package runs the broker as a systemd service, `mosquitto.service`. It is started as root, reads
`/etc/mosquitto/mosquitto.conf`, which includes every `*.conf` file in `/etc/mosquitto/conf.d`, and then runs as the
user `mosquitto`. The broker must be able to read its certificate and key as that user, and nobody else should be
able to read the key.

**1. Create the certificates** on any machine with OpenSSL 3 (`openssl version`; Ubuntu 22.04 and Debian 12 have
it), as above:

```sh
(umask 077; bash create-certs.sh mybroker.lan)
```

**2. Put the broker's files in a directory only root and the broker can read.** The package already creates
`/etc/mosquitto/certs`; `install` copies a file and sets its owner and mode in one step:

```sh
sudo install -d -o root -g mosquitto -m 750 /etc/mosquitto/certs
sudo install -o root -g mosquitto -m 644 ssl/ca.crt ssl/server.crt /etc/mosquitto/certs/
sudo install -o root -g mosquitto -m 640 ssl/server.key /etc/mosquitto/certs/
```

| Path | Owner | Mode | Why |
|---|---|---|---|
| `/etc/mosquitto/certs` | `root:mosquitto` | `750` (`drwxr-x---`) | Only root can change it; the broker can list and open what is in it; nobody else can. |
| `ca.crt`, `server.crt` | `root:mosquitto` | `644` | Certificates are public, but only root may replace them. |
| `server.key` | `root:mosquitto` | `640` (`-rw-r-----`) | Root can write it, the broker can read it, nobody else can. |

`ca.key` does not belong on the broker: it is only needed to sign new certificates. Keep it, and the rest of the
`ssl` directory, where you created it, readable by you alone (`chmod 700 ssl`, and `chmod 600 ssl/*.key` if the
script ran without the `umask`), or move it off the machine.

**3. Configure the TLS listeners** in a file of their own in `conf.d`, with absolute paths:

```sh
sudo tee /etc/mosquitto/conf.d/tls.conf > /dev/null <<'EOF'
# MQTT over TLS (ssl://host:8883)
listener 8883
cafile /etc/mosquitto/certs/ca.crt
certfile /etc/mosquitto/certs/server.crt
keyfile /etc/mosquitto/certs/server.key
tls_version tlsv1.2

# WebSockets over TLS (wss://host:8084)
listener 8084
protocol websockets
cafile /etc/mosquitto/certs/ca.crt
certfile /etc/mosquitto/certs/server.crt
keyfile /etc/mosquitto/certs/server.key
tls_version tlsv1.2
EOF
sudo chown root:mosquitto /etc/mosquitto/conf.d/tls.conf
sudo chmod 640 /etc/mosquitto/conf.d/tls.conf
```

The configuration holds no secrets, only paths, but `640` keeps it as protected as the files it names, and is needed
once it names a password file or holds other settings you would rather not show. Add `require_certificate true` (and
`use_identity_as_username true`) to a listener for mutual TLS, as in
[Requiring client certificates](#requiring-client-certificates). As on Windows, Mosquitto 2.x refuses clients
without a user name unless `allow_anonymous true` is set; for anything reachable from other machines, use a password
file (`password_file /etc/mosquitto/passwd`, created with `sudo mosquitto_passwd -c /etc/mosquitto/passwd <user>`,
then `chown root:mosquitto` and `chmod 640` it like the key) or client certificates instead.

**4. Restart the broker and check it:**

```sh
sudo systemctl restart mosquitto
systemctl status mosquitto
sudo journalctl -u mosquitto -e          # why it did not start, if it did not
sudo ss -ltnp | grep -E ':8883|:8084'    # the TLS listeners are open
```

A key the broker cannot read shows in the journal as `Error: Unable to load server key file`; check the owner, group
and mode above, and that the directory is `750` rather than `700`. To watch connections, stop the service and run the
broker in a terminal with the same configuration (it still switches to the `mosquitto` user):

```sh
sudo systemctl stop mosquitto
sudo mosquitto -c /etc/mosquitto/mosquitto.conf -v
```

Open the ports in the firewall if clients connect from other machines, for example with `ufw`:

```sh
sudo ufw allow 8883/tcp
sudo ufw allow 8084/tcp
```

Then check from a client machine with the CA certificate (and, for mutual TLS, the client certificate and key), as in
[Starting and checking the broker](#starting-and-checking-the-broker):

```sh
mosquitto_pub -h mybroker.lan -p 8883 --cafile ssl/ca.crt -t test/hello -m hi
openssl s_client -connect mybroker.lan:8883 -CAfile ssl/ca.crt -verify_hostname mybroker.lan -brief
```

**5. Protect the client's files too.** On a client machine, `client.key` is the client's identity: keep it readable
only by the user the application runs as (`chmod 600`), and `ca.crt` and `client.crt` at `644`.

## Connecting from the client

Put the files where the application can find them. With Maven or Gradle, `src/main/resources/ssl/ca.crt` is the
classpath resource `ssl/ca.crt`, both when run from the IDE and from the JAR. If your files are in `res/ssl`, mark
`res` as a resources folder (or add it to `<resources>` in the POM) and use the names `ssl/ca.crt` and so on.

Then choose one of two ways to build the TLS context. Both have been tested against Mosquitto with every case in
[Troubleshooting](#troubleshooting).

### Option A: Netty's `SslContextBuilder` (no BouncyCastle)

The client accepts a Netty `SslContext`, and Netty reads PEM files itself. That covers every file the batch file
writes, so no extra library is needed:

```java
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

// Files on disk
SslContext sslContext = SslContextBuilder.forClient()
        .protocols("TLSv1.2")
        .trustManager(new File("ssl/ca.crt"))
        .keyManager(new File("ssl/client.crt"), new File("ssl/client.key"))   // only for mutual TLS
        .build();

// Or classpath resources (works from a JAR)
ClassLoader loader = MyApp.class.getClassLoader();
try (InputStream ca = loader.getResourceAsStream("ssl/ca.crt");
     InputStream crt = loader.getResourceAsStream("ssl/client.crt");
     InputStream key = loader.getResourceAsStream("ssl/client.key")) {
    sslContext = SslContextBuilder.forClient()
            .protocols("TLSv1.2")
            .trustManager(ca)
            .keyManager(crt, key)          // keyManager(crt, key, password) for an encrypted key
            .build();
}

MqttConnectOptions options = new MqttConnectOptions();
options.setServerURI("ssl://localhost:8883");
options.setSslContext(sslContext);
options.setHttpsHostnameVerificationEnabled(true);
```

Netty reads PKCS#8 keys (`BEGIN PRIVATE KEY`, OpenSSL 3's default), plain or encrypted, on its own. Keys in the
OpenSSL 1.x traditional format (`BEGIN RSA PRIVATE KEY`) need BouncyCastle on the classpath, which Netty then uses
automatically; without it the build fails with `File does not contain valid private key`. To convert such a key to
PKCS#8 instead:

```bat
openssl pkcs8 -topk8 -nocrypt -in client.key -out client-pkcs8.key
```

### Option B: `SslUtil` (a JDK `SSLContext`, with BouncyCastle)

[`examples/ssl/SslUtil.java`](examples/ssl/SslUtil.java) is the cleaned-up version of the test client's helper.
It builds a standard `javax.net.ssl.SSLContext`, which also suits code that needs a JDK context, such as an
`SSLSocketFactory` for Paho (`context.getSocketFactory()`). It reads certificates with the JDK and private keys with
BouncyCastle, in all four forms OpenSSL writes: PKCS#8, encrypted PKCS#8, traditional, and encrypted traditional.

It needs BouncyCastle's PKIX library, which brings in `bcprov` and `bcutil`:

```xml
<dependency>
    <groupId>org.bouncycastle</groupId>
    <artifactId>bcpkix-jdk18on</artifactId>
    <version>1.82</version>
</dependency>
```

```java
// Trust the CA only
SSLContext context = SslUtil.forServerAuth("ssl/ca.crt", SslUtil.TLS_V1_2);

// or mutual TLS; pass the password for an encrypted key, or null
context = SslUtil.forMutualAuth("ssl/ca.crt", "ssl/client.crt", "ssl/client.key", null, SslUtil.TLS_V1_2);

MqttConnectOptions options = new MqttConnectOptions();
options.setServerURI("ssl://localhost:8883");
options.setSslContext(context);
options.setHttpsHostnameVerificationEnabled(true);
```

Each name is tried as a classpath resource first (`ssl/ca.crt` or `/ssl/ca.crt`), then as a file path, so the same
code works from the IDE, from a JAR and with files outside the application. A CA file may hold several certificates
(a bundle), and all of them are trusted. Problems are thrown as `IOException` or `GeneralSecurityException` with the
file name, rather than logged with `null` returned.

The test client's wrapper then becomes:

```java
private static SSLContext getSSLContext(String serverUrl, String caFile, boolean caCertOnly) {
    try {
        return caCertOnly
                ? SslUtil.forServerAuth(ssl_resource_folder + caFile, SslUtil.TLS_V1_2)
                : SslUtil.forMutualAuth(ssl_resource_folder + caFile,
                        ssl_resource_folder + m_clientCrtFilePath,
                        ssl_resource_folder + m_clientKeyFilePath,
                        keyPassword,                      // char[], or null if the key is not encrypted
                        SslUtil.TLS_V1_2);
    } catch (IOException | GeneralSecurityException e) {
        l4j.fatal("Cannot create the SSLContext for " + serverUrl, e);
        return null;
    }
}

SSLContext emq_sslContext = getSSLContext("ssl://broker.emqx.io", m_caOnlyFilePath, true);
```

[`examples/ssl/SslExample.java`](examples/ssl/SslExample.java) is a runnable version that connects and reports the
outcome. [`examples/pom.xml`](examples/pom.xml) builds it into `examples/target/evanino-mqtt-java-examples.jar`, with
its dependencies (this library, Netty, Guava and BouncyCastle) in `examples/target/lib`. The examples are a separate
project, not part of the library build, so the library does not depend on BouncyCastle. They use the library from
the local Maven repository, so install it first:

```bat
rem in the project root
mvn clean install -Dgpg.skip=true -DskipTests
rem in examples/
mvn package
java -jar target\evanino-mqtt-java-examples.jar ssl://localhost:8883 ssl/ca.crt
java -jar target\evanino-mqtt-java-examples.jar ssl://localhost:8883 ssl/ca.crt ssl/client.crt ssl/client.key
```

`mvn exec:java -Dexec.args="ssl://localhost:8883 ssl/ca.crt"` runs it without building the jar.

### Brokers with a certificate from a public CA

A broker whose certificate comes from a public CA (Let's Encrypt, DigiCert and so on) is already trusted by the JVM.
Then no context or CA file is needed: with an `ssl://` or `wss://` URL and no `setSslContext(...)`, the client uses
the JVM's default context. Turn on hostname verification either way:

```java
options.setServerURI("ssl://broker.example.com:8883");
options.setHttpsHostnameVerificationEnabled(true);
```

A CA file is only needed for brokers whose certificates are signed by a private CA, as in the development setup
here, or for test services that publish their own CA.

## Hostname verification

A trusted CA only proves that the certificate was issued by that CA, not that it was issued for the host you meant
to reach: any certificate from the same CA would pass. Hostname verification closes that gap by checking that the
broker's certificate names the host in the broker URL.

### On the broker

Mosquitto does not verify host names. It presents its certificate, and the client checks it. The broker's part is to
present a certificate that names every host clients use:

- **Put every name and address in the subject alternative name.** Clients check the SAN, not the CN, whenever the
  certificate has one. Names are `DNS:` entries and IP addresses are `IP:` entries; an address written as a `DNS:`
  entry, or only in the CN, does not match.
- **Check a certificate before deploying it:**

  ```bat
  openssl x509 -in server.crt -noout -ext subjectAltName
  openssl verify -CAfile ca.crt -verify_hostname mybroker.lan server.crt
  openssl verify -CAfile ca.crt -verify_ip 127.0.0.1 server.crt
  ```

The broker side has a related check for clients: with `require_certificate true`, a client must present a
certificate signed by the broker's `cafile` CA. Host names play no part in it, because the broker cannot know the
client's name. With `use_identity_as_username true`, the certificate's CN becomes the client's identity for ACLs.

### In the client

Hostname verification is **off by default** in this client, as it was in Paho before 1.2.1. Turn it on with:

```java
options.setHttpsHostnameVerificationEnabled(true);
```

or with `options.enablePahoMode(true)`, which turns it on along with Paho's other defaults. It sets the TLS engine's
endpoint identification to `HTTPS` (RFC 2818), so the check happens during the handshake: the host in the broker URL
must match a `DNS:` entry of the SAN (wildcards such as `*.example.com` allowed), or an `IP:` entry for an address.
It works the same for `ssl://` and `wss://`, and whichever way the context was built (option A or B, or the JVM
default).

With the certificate from the batch file (`DNS:localhost` only):

| URL | Result |
|---|---|
| `ssl://localhost:8883` | Connects. |
| `ssl://127.0.0.1:8883` | Fails: `No subject alternative names matching IP address 127.0.0.1 found`. |
| `ssl://localhost:8883`, broker certificate from another CA | Fails: `unable to find valid certification path to requested target`. |

When it fails, the connect token fails with `MqttException` reason code 32103 (`REASON_CODE_SERVER_CONNECT_ERROR`),
with the `SSLHandshakeException` in its cause chain, and the broker logs `ssl/tls alert certificate unknown`.

For a rule of your own, such as accepting a fixed list of names, set a `HostnameVerifier`. As in Paho, it is only
used while HTTPS hostname verification is off, and runs after the handshake with the host from the URL and the TLS
session:

```java
options.setHttpsHostnameVerificationEnabled(false);
options.setSSLHostnameVerifier((host, session) -> host.equals("localhost") || host.equals("127.0.0.1"));
```

If it returns `false`, the connection is closed before CONNECT is sent, and the connect fails with an
`SSLPeerUnverifiedException` in its cause chain.

## What changed from the original helper

The test client's `SslUtil` worked for some combinations and failed for others. These were found by running it
against Mosquitto with the files from the batch file:

| Original | Effect | Now |
|---|---|---|
| Private keys were always cast to `PEMKeyPair`. | Mutual TLS with an OpenSSL 3 key (`BEGIN PRIVATE KEY`, what the batch file writes) failed with `ClassCastException: PrivateKeyInfo cannot be cast to PEMKeyPair`, and the method returned `null`. | All four key formats are read; an encrypted key without a password is reported as such. |
| In a JAR, the CA-only method read the CA file with `new InputStreamReader(in).toString()`, which gives the reader's object name, not the file. | From a JAR, the CA-only context trusted nothing, and every connection failed with `the trustAnchors parameter must be non-empty`. | Certificates are read with `CertificateFactory.generateCertificates`, which also handles bundles. |
| The IDE and JAR code paths looked resources up in different places (`ssl/ca.crt` against `org/example/ssl/ca.crt`), and whether it ran from a JAR was detected from where `Main.class` was found. | The same names behaved differently in the IDE and in the JAR, and the helper failed with a `NullPointerException` in any package without a `Main` class. | One lookup: classpath resource, then file. |
| Errors were logged and `null` returned. | The caller only learnt that it failed. | Errors are thrown with the file name and the reason. |
| `Security.addProvider(new BouncyCastleProvider())` on every call. | BouncyCastle was installed for the whole JVM. | BouncyCastle is passed only to the key decryptors. |
| `getSocketFactory(...)` used `SSLContext.getInstance("TLSv1")`. | A listener with `tls_version tlsv1.2` refuses TLS 1.0. | Not needed by this client; use `context.getSocketFactory()` for Paho. |
| The wrapper checked the field `m_caFilePath` rather than its `caFilePath` parameter, and ignored `caCertOnly`. | It could report a missing CA file that was set, or the reverse. | See the wrapper above. |

## Troubleshooting

Messages as reported in the connect token's cause chain (client) and in the Mosquitto log with `-v` (broker):

| Client | Broker | Cause |
|---|---|---|
| `No subject alternative names matching IP address ... found` or `No name matching ... found` | `ssl/tls alert certificate unknown` | Hostname verification: the certificate does not name the host in the URL. Connect with a name in the SAN, or add the name to the SAN. |
| `unable to find valid certification path to requested target` | `ssl/tls alert certificate unknown` | The client does not trust the CA that signed the broker's certificate: wrong or old `ca.crt`, often after re-running the batch file. |
| `Channel closed before CONNACK received` | `peer did not return a certificate` | The listener has `require_certificate true` and the client sent no certificate. Use mutual TLS. |
| `the trustAnchors parameter must be non-empty` | | The trust store is empty: the CA file was read as empty or not at all. |
| `File does not contain valid private key` (option A) | | A traditional-format key without BouncyCastle on the classpath. Add BouncyCastle, or convert the key to PKCS#8. |
| `KeyException: The private key in ... is encrypted; a password is needed` (option B) | | Pass the key's password. |
| `KeyException: Cannot decrypt the private key in ...` (option B) | | Wrong password. |
| `FileNotFoundException: ... is neither a classpath resource nor a file` (option B) | | The name is wrong, or the folder is not on the classpath. |
| `protocol error` from `mosquitto_pub` | `protocol error` | A command-line client's hostname check failed (for example `-h 127.0.0.1` against `DNS:localhost`). |
