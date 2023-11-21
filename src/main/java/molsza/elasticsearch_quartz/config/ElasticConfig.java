package molsza.elasticsearch_quartz.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Slf4j
@Configuration
@Setter
@ConfigurationProperties(prefix = "elastic")
public class ElasticConfig {

  private String serverUrl;
  private String user;
  private String password;
  private int port = 9200;
  private String scheme = "https";
  private String serverPathPrefix;
  private String proxy;

  @Bean
  @ConditionalOnMissingBean
  public ElasticsearchClient elasticsearchTemplate() {

    log.info("Connecting to elasticsearch {}://{}:{}", scheme, serverUrl, port);

    final CredentialsProvider credentialsProvider =
        new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(user, password));

    RestClientBuilder builder = RestClient.builder(
            new HttpHost(serverUrl, port, scheme))
        .setHttpClientConfigCallback(httpClientBuilder -> {
          try {
            if (StringUtils.hasText(proxy)) {
              log.info("Connecting to elasticsearch through proxy {}", proxy);
              httpClientBuilder = httpClientBuilder.setProxy(HttpHost.create(proxy));
            }
            return httpClientBuilder
                .setSSLContext(SSLContexts.custom()
                    .loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
                    .build()).setSSLHostnameVerifier((host, sslSession) -> true)
                .setDefaultCredentialsProvider(credentialsProvider);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    if (StringUtils.hasText(serverPathPrefix)) builder.setPathPrefix(serverPathPrefix);
    builder.setCompressionEnabled(true);
    builder.setRequestConfigCallback(r -> r.setConnectionRequestTimeout(45000));
    ElasticsearchTransport transport = new RestClientTransport(
        builder.build(),
        new JacksonJsonpMapper()
    );

    return new ElasticsearchClient(transport);
  }
}
