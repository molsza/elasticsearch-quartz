package molsza.elasticsearch_quartz.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import molsza.elasticsearch_quartz.job_store.ElasticsearchJobStore;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.JobStore;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

import java.util.concurrent.Executors;

import static org.quartz.impl.StdSchedulerFactory.PROP_JOB_STORE_CLASS;

@Configuration
public class QuartzConfig {

  @Value("${quartz.enabled:false}")
  boolean enableQuartz;

  final ApplicationContext applicationContext;

  public QuartzConfig(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  @Bean
  SpringBeanJobFactory createSpringBeanJobFactory() {

    return new SpringBeanJobFactory() {

      @Override
      protected Object createJobInstance
          (final TriggerFiredBundle bundle) throws Exception {

        final Object job = super.createJobInstance(bundle);

        applicationContext
            .getAutowireCapableBeanFactory()
            .autowireBean(job);

        return job;
      }
    };
  }

  @Bean
  @ConditionalOnMissingBean
  public SchedulerFactoryBean createSchedulerFactory
      (SpringBeanJobFactory springBeanJobFactory, ElasticsearchClient client) throws SchedulerException {

    var treadFactory = new CustomizableThreadFactory();
    treadFactory.setThreadGroupName("quartz");
    SchedulerFactoryBean schedulerFactory
        = new SchedulerFactoryBean();
    schedulerFactory.setTaskExecutor(Executors.newSingleThreadExecutor(treadFactory));
    schedulerFactory.setAutoStartup(enableQuartz);
    schedulerFactory.setWaitForJobsToCompleteOnShutdown(true);
    System.setProperty(PROP_JOB_STORE_CLASS, ElasticsearchJobStore.class.getName());
    schedulerFactory.setSchedulerFactory(new StdSchedulerFactory() {
      @Override
      protected Scheduler instantiate(QuartzSchedulerResources rsrcs, QuartzScheduler qs) {
        JobStore jobStore = rsrcs.getJobStore();
        if (jobStore instanceof ElasticsearchJobStore) {
          ((ElasticsearchJobStore) jobStore).initElastic(client, "quartz");
        }
        return super.instantiate(rsrcs, qs);
      }
    });

    springBeanJobFactory.setApplicationContext(applicationContext);
    schedulerFactory.setJobFactory(springBeanJobFactory);

    return schedulerFactory;
  }
}
