package molsza.elasticsearch_quartz.job_store;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static molsza.elasticsearch_quartz.job_store.TriggerWrapper.*;

@Slf4j
public class ElasticsearchJobStore implements JobStore {
  private static final String JOB_TYPE = "job";
  private static final String TRIGGER_TYPE = "trigger";
  protected HashMap<String, Calendar> calendarsByName = new HashMap<String, Calendar>(25);

  // Properties
  private String indexName;

  // Internal variables
  private SchedulerSignaler signaler;
  private ElasticsearchClient client;
  private int poolSize;
  private long misfireThreshold;

  public void initElastic(ElasticsearchClient client, String indexName) {
    this.client = client;
    this.indexName = indexName;
    createIndex();
  }

  public void createIndex() {
    try {
      if (!client.indices().exists(ExistsRequest.of(r -> r.index(indexName))).value()) {
        log.info("Creating index {}", indexName);
        boolean result = client.indices().create(fn -> fn.index(indexName).mappings(this::indexMapping)).acknowledged();
        if (!result) {
          throw new IllegalStateException("Index " + indexName + " has not been created");
        }
      } else {
        log.debug("Index {} already exists", indexName);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  protected ObjectBuilder<TypeMapping> indexMapping(TypeMapping.Builder builder) {
    Function<Property.Builder, ObjectBuilder<Property>> fn = b -> {
      b.keyword(a -> a.store(true));
      return b;
    };

    Function<Property.Builder, ObjectBuilder<Property>> number = b -> {
      b.integer(f -> f.index(true));
      return b;
    };

    Function<Property.Builder, ObjectBuilder<Property>> timestamp = b -> {
      b.date(f -> f.index(true));
      return b;
    };

    return builder.dynamic(DynamicMapping.False)
        .properties("timesTriggered", number)
        .properties("state", number)
        .properties("repeatInterval", number)
        .properties("repeatCount", number)
        .properties("priority", number)
        .properties("maxExecutionTime", number)
        .properties("executionTimeout", timestamp)
        .properties("startTime", timestamp)
        .properties("endTime", timestamp)
        .properties("previousFireTime", timestamp)
        .properties("nextFireTime", timestamp)
        .properties("group", fn)
        .properties("jobGroup", fn)
        .properties("jobClass", fn)
        .properties("jobName", fn)
        .properties("name", fn)
        .properties("triggerClass", fn)
        .properties("type", fn)
        .properties("dataMap", b -> b.object(v -> v.dynamic(DynamicMapping.True)))
        .properties("cronExpression", fn);
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
    this.signaler = signaler;

  }

  private void checkSetting(Object setting, String propertyKey) throws SchedulerConfigException {
    if (setting == null) {
      throw new SchedulerConfigException("The property '" + propertyKey + "' must be set");
    }
  }

  @Override
  public void schedulerStarted() throws SchedulerException {

  }

  @Override
  public void schedulerPaused() {
    //nop
  }

  @Override
  public void schedulerResumed() {
    //nop
  }

  @Override
  public void shutdown() {
    //nop
  }

  @Override
  public boolean supportsPersistence() {
    return true;
  }

  @Override
  public long getEstimatedTimeToReleaseAndAcquireTrigger() {
    return 10;
  }

  @Override
  public boolean isClustered() {
    return true;
  }

  @Override
  public synchronized void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
    storeJob(newJob, false);
    storeTrigger(newTrigger, false);
  }

  @Override
  public synchronized void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
    JobKey key = newJob.getKey();

    JobWrapper jobWrapper = new JobWrapper();
    jobWrapper.setDescription(newJob.getDescription());
    jobWrapper.setName(key.getName());
    jobWrapper.setGroup(key.getGroup());
    jobWrapper.setJobClass(newJob.getJobClass().getName());
    jobWrapper.setDataMap(newJob.getJobDataMap().getWrappedMap());

    try {
      IndexResponse response = client.index(fn -> fn.opType(replaceExisting ? OpType.Index : OpType.Create).index(indexName).id(key.toString()).document(jobWrapper));
      if (response.result() == Result.Created || response.result() == Result.Updated) {
        log.info("Successfully stored job '{}'", key);
      }
    } catch (ElasticsearchException e) {
      if (e.error().type().equals("version conflict")) {
        throw new ObjectAlreadyExistsException("Job " + key + " already exists");
      } else throw e;
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace) throws ObjectAlreadyExistsException, JobPersistenceException {
    for (JobDetail job : triggersAndJobs.keySet()) {
      storeJob(job, replace);
      Set<? extends Trigger> triggers = triggersAndJobs.get(job);
      for (Trigger trigger : triggers) {
        storeTrigger((OperableTrigger) trigger, replace);
      }
    }
  }

  @Override
  public synchronized boolean removeJob(JobKey jobKey) throws JobPersistenceException {

    try {
      DeleteResponse response = client.delete(d -> d.index(indexName).id(jobKey.toString()));
      if (response.result() == Result.Deleted) {
        log.debug("Successfully removed job {}", jobKey);
        return true;
      }
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
    return false;
  }

  @Override
  public synchronized boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
    boolean failed = false;
    for (JobKey key : jobKeys) {
      failed |= !removeJob(key);
    }
    return !failed;
  }

  @Override
  public synchronized JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    try {
      var response = client.get(g -> g.index(indexName).id(jobKey.toString()), JobWrapper.class);
      if (response.found()) {
        return JobUtils.fromWrapper(response.source());
      } else return null;
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws JobPersistenceException {
    TriggerKey key = newTrigger.getKey();

    TriggerWrapper triggerWrapper = TriggerUtils.toTriggerWrapper(newTrigger, STATE_WAITING);

    storeTrigger(key.toString(), triggerWrapper, replaceExisting);
  }

  private void storeTrigger(String key, TriggerWrapper triggerWrapper, boolean replaceExisting) throws JobPersistenceException {
    try {
      IndexResponse response = client.index(fn -> fn.opType(replaceExisting ? OpType.Index : OpType.Create).index(indexName).id(key).document(triggerWrapper));
      if (response.result() == Result.Created || response.result() == Result.Updated) {
        log.info("Successfully stored trigger '{}'", key);
      }
    } catch (ElasticsearchException e) {
      if (e.error().type().equals("version conflict")) {
        throw new ObjectAlreadyExistsException("Trigger " + key + " already exists");
      } else throw e;
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    try {
      DeleteResponse response = client.delete(d -> d.index(indexName).id(triggerKey.toString()));
      if (response.result() == Result.Deleted) {
        log.debug("Successfully removed trigger {}", triggerKey);
        return true;
      }
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
    return false;
  }

  @Override
  public synchronized boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
    boolean failed = false;
    for (TriggerKey key : triggerKeys) {
      failed |= !removeTrigger(key);
    }
    return !failed;
  }

  @Override
  public synchronized boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
    boolean removed = removeTrigger(triggerKey);
    storeTrigger(newTrigger, true);
    return removed;
  }

  @Override
  public synchronized OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    try {
      var response = client.get(g -> g.index(indexName).id(triggerKey.toString()), TriggerWrapper.class);
      if (response.found()) {
        return TriggerUtils.fromWrapper(response.source());
      } else return null;
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
    return retrieveJob(jobKey) != null;
  }

  @Override
  public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
    return retrieveTrigger(triggerKey) != null;
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {

  }

  @Override
  public void storeCalendar(String name,
                            Calendar calendar, boolean replaceExisting, boolean updateTriggers)
      throws ObjectAlreadyExistsException {

    calendar = (Calendar) calendar.clone();

    Object obj = calendarsByName.get(name);

    if (obj != null && !replaceExisting) {
      throw new ObjectAlreadyExistsException(
          "Calendar with name '" + name + "' already exists.");
    } else if (obj != null) {
      calendarsByName.remove(name);
    }

    calendarsByName.put(name, calendar);

//      if(obj != null && updateTriggers) {
//        for (org.quartz.simpl.TriggerWrapper tw : getTriggerWrappersForCalendar(name)) {
//          OperableTrigger trig = tw.getTrigger();
//          boolean removed = timeTriggers.remove(tw);
//
//          trig.updateWithNewCalendar(calendar, getMisfireThreshold());
//
//          if (removed) {
//            timeTriggers.add(tw);
//          }
//        }
  }

  @Override
  public boolean removeCalendar(String calName) throws JobPersistenceException {
    calendarsByName.remove(calName);
    return false;
  }

  @Override
  public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
    return calendarsByName.get(calName);
  }

  @Override
  public int getNumberOfJobs() throws JobPersistenceException {
    try {
      var response = client.count(c -> c.index(indexName).query(q -> q.match(m -> m.field("type").query("job"))));
      return Math.toIntExact(response.count());
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public int getNumberOfTriggers() throws JobPersistenceException {
    try {
      var response = client.count(c -> c.index(indexName).query(q -> q.match(m -> m.field("type").query("trigger"))));
      return Math.toIntExact(response.count());
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public int getNumberOfCalendars() throws JobPersistenceException {
    return 0;
  }

  @Override
  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    if (matcher.getCompareWithOperator() != StringMatcher.StringOperatorName.EQUALS) {
      throw new JobPersistenceException("Only Equals is supported");
    }
    try {
      var response = client.search(c -> c.size(1_000).index(indexName)
          .query(q -> q.bool(b -> b.must(
                  Query.of(b1 -> b1.term(t -> t.field("type").value("job"))),
                  Query.of(b1 -> b1.term(t -> t.field("group").value(matcher.getCompareToValue())))
              )
          )), JobWrapper.class);
      return response.hits().hits().stream().map(j -> JobKey.jobKey(j.source().getName(), j.source().getGroup())).collect(Collectors.toSet());
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    return null;
  }

  @Override
  public List<String> getJobGroupNames() throws JobPersistenceException {
    try {
      var response = client.search(c -> c.size(0).index(indexName)
          .query(q -> q.term(t -> t.field("type").value("job")))
          .aggregations("group", a -> a.terms(t -> t.field("group"))), ObjectNode.class);
      return response.aggregations().get("group").sterms().buckets().array().stream().map(m -> m.key().stringValue()).collect(Collectors.toList());
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    return null;
  }

  @Override
  public List<String> getCalendarNames() throws JobPersistenceException {
    return null;
  }

  @Override
  public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
    try {
      return client.search(s -> s.query(Query.of(q -> q.bool(b -> b.must(
          Query.of(f1 -> f1.term(t -> t.field("type").value("trigger"))),
          Query.of(f2 -> f2.term(t -> t.field("jobGroup").value(jobKey.getGroup()))),
          Query.of(f2 -> f2.term(t -> t.field("jobName").value(jobKey.getName())))
      )))).index(indexName), TriggerWrapper.class).hits().hits().stream().map(s -> TriggerUtils.fromWrapper(s.source())).collect(Collectors.toList());
    } catch (IOException | ElasticsearchException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
    return null;
  }

  @Override
  public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {

  }

  @Override
  public synchronized void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    try {
      var response = client.get(g -> g.index(indexName).id(triggerKey.toString()), TriggerWrapper.class);
      if (response.found()) {
        response.source().setState(STATE_PAUSE);
        storeTrigger(triggerKey.toString(), response.source(), true);
      }
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    return null;
  }

  @Override
  public void pauseJob(JobKey jobKey) throws JobPersistenceException {

  }

  @Override
  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
    return null;
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    try {
      var response = client.get(g -> g.index(indexName).id(triggerKey.toString()), TriggerWrapper.class);
      if (response.found()) {
        response.source().setState(STATE_WAITING);
        storeTrigger(triggerKey.toString(), response.source(), true);
      }
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    return null;
  }

  /**
   * This method returns all paused triggers. Not only groups.
   *
   * @return
   * @throws JobPersistenceException
   */
  @Override
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    try {
      var response = client.search(g -> g.index(indexName).query(f -> f.bool(q -> q.must(new Query.Builder().match(m ->
              m.field("type").query("trigger")).build(),
          new Query.Builder().match(m ->
              m.field("state").query(STATE_PAUSE)).build()))), TriggerWrapper.class);
      return response.hits().hits().stream().map(s -> s.id()).collect(Collectors.toSet());
    } catch (Exception e) {
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  @Override
  public void resumeJob(JobKey jobKey) throws JobPersistenceException {

  }

  @Override
  public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    return null;
  }

  @Override
  public void pauseAll() throws JobPersistenceException {

  }

  @Override
  public void resumeAll() throws JobPersistenceException {

  }

  @Override
  public synchronized List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
    try {
      client.indices().flush(s -> s.index(indexName));
      var response = client.search(s -> getSearchBody(s, noLaterThan, timeWindow).index(indexName), TriggerWrapper.class);
      BulkRequest.Builder b = new BulkRequest.Builder();
      b.index(indexName);
      if (response.hits().total().value() > 0) {
        //something is not working with selecting only trigers with 0, so checking again
        response.hits().hits().stream().filter(v -> v.source().getNextFireTime() > 0).limit(maxCount).forEach(h -> b.operations(o -> o.update(u -> u.id(h.id())
            .action(v -> v.doc(Map.of("state", STATE_ACQUIRED, "executionTimeout", h.source().getMaxExecutionTime() > 0 ? System.currentTimeMillis() + h.source().getMaxExecutionTime() : 0))))));
        BulkResponse r = client.bulk(b.build());
        if (log.isDebugEnabled()) {
          log.debug("Acquiring triggers {}", r.items().stream().map(m -> m.id()).collect(Collectors.toList()));
        }
        r.items().stream().filter(i -> i.error() != null).forEach(i -> log.info("Trigger couldn't be updated: {}", i.error().reason()));
        var idSet = r.items().stream().filter(i -> i.error() == null).map(i -> i.id()).collect(Collectors.toList());
        if (!idSet.isEmpty()) {
          var res = client.mget(g -> g.index(indexName).ids(idSet), TriggerWrapper.class);
          return res.docs().stream().filter(v -> v.result().source().getNextFireTime() > 0).map(h -> TriggerUtils.fromWrapper(h.result().source())).collect(Collectors.toList());
        }
      }
      return Collections.emptyList();
    } catch (Exception e) {
      log.error("Obtaining next trigger failed", e);
      throw new JobPersistenceException(e.getMessage(), e);
    }
  }

  private SearchRequest.Builder getSearchBody(SearchRequest.Builder s, long noLaterThan, long timeWindow) {
    return s.query(q -> q.bool(v -> v.should(
        new BoolQuery.Builder().must(
            new Query.Builder().match(m ->
                m.field("type").query("trigger")).build(),
            new Query.Builder().match(m ->
                m.field("state").query(STATE_WAITING)).build(),
            new Query.Builder().range(m -> m.field("nextFireTime").gt(JsonData.of(0)).lte(JsonData.of(noLaterThan + timeWindow))).build()
        ).build()._toQuery(),
        new BoolQuery.Builder().must(
            new Query.Builder().match(m ->
                m.field("type").query("trigger")).build(),
            new Query.Builder().match(m ->
                m.field("state").query(STATE_ACQUIRED)).build(),
            new Query.Builder().range(m -> m.field("executionTimeout").lte(JsonData.of(noLaterThan))).build(),
            new Query.Builder().range(m -> m.field("nextFireTime").gt(JsonData.of(0)).lte(JsonData.of(noLaterThan + timeWindow))).build()
        ).build()._toQuery()
    )));
  }

  @Override
  public synchronized void releaseAcquiredTrigger(OperableTrigger trigger) {
    try {
      var res = client.update(u -> u.id(trigger.getKey().toString()).index(indexName).doc(Collections.singletonMap("state", STATE_WAITING)), TriggerWrapper.class);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  private TriggerFiredResult fireError(String message) {
    RuntimeException exception = new RuntimeException(message);
    return new TriggerFiredResult(exception);
  }

  private TriggerFiredResult fireError() {
    TriggerFiredBundle bundle = null;
    return new TriggerFiredResult(bundle);
  }

  @Override
  public synchronized List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
    List<TriggerFiredResult> fireResult = new ArrayList<>();

    for (OperableTrigger trigger : triggers) {
      TriggerKey key = trigger.getKey();
      log.debug("Firing trigger {}", key);

      // Get the trigger to retrieve the version number
      try {
        var response = client.get(g -> g.index(indexName).id(key.toString()), TriggerWrapper.class);
        if (response.found() && response.source().getState() == STATE_ACQUIRED) {
          trigger.triggered(null);
          TriggerWrapper triggerWrapper = TriggerUtils.toTriggerWrapper(trigger, response.source().getState());
          triggerWrapper.setTimesTriggered(response.source().getTimesTriggered() + 1);
          triggerWrapper.setExecutionTimeout(response.source().getExecutionTimeout());
          storeTrigger(key.toString(), triggerWrapper, true);
          TriggerFiredBundle triggerFiredBundle = getTriggeredFireBundle(triggerWrapper);
          fireResult.add(new TriggerFiredResult(triggerFiredBundle));
        } else continue;
      } catch (Exception e) {
        throw new JobPersistenceException(e.getMessage(), e);
      }
    }
    return fireResult;
  }


  private TriggerFiredBundle getTriggeredFireBundle(TriggerWrapper triggerWrapper) throws JobPersistenceException {
    JobKey jobKey = new JobKey(triggerWrapper.getJobName(), triggerWrapper.getJobGroup());
    JobDetail job = retrieveJob(jobKey);
    OperableTrigger trigger = TriggerUtils.fromWrapper(triggerWrapper);

    Date scheduledFireTime = trigger.getPreviousFireTime();
    trigger.triggered(null);
    Date previousFireTime = trigger.getPreviousFireTime();

    return new TriggerFiredBundle(job, trigger, null, false, new Date(), scheduledFireTime, previousFireTime, trigger.getNextFireTime());
  }

  @Override
  public synchronized void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
    List<OperableTrigger> triggersForJob = null;
    log.debug("Job {} completed and was triggered by {}", jobDetail.getKey(), trigger.getKey());

    try {
      boolean signal = true;
      switch (triggerInstCode) {
        case NOOP:
        case SET_TRIGGER_ERROR:
          updateTrigger(trigger.getKey(), STATE_WAITING, null);
          break;

        case DELETE_TRIGGER:
          signal = removeTrigger(trigger.getKey());
          break;

        case SET_TRIGGER_COMPLETE:
          updateTrigger(trigger.getKey(), STATE_COMPLETED, null);
          break;

        case SET_ALL_JOB_TRIGGERS_COMPLETE:
          triggersForJob = getTriggersForJob(jobDetail.getKey());
          for (OperableTrigger triggerForJob : triggersForJob) {
            updateTrigger(triggerForJob.getKey(), STATE_COMPLETED, null);
          }
          break;

        case SET_ALL_JOB_TRIGGERS_ERROR:
          triggersForJob = getTriggersForJob(jobDetail.getKey());
          for (OperableTrigger triggerForJob : triggersForJob) {
            updateTrigger(triggerForJob.getKey(), STATE_WAITING, null);
          }
          break;

        case RE_EXECUTE_JOB:
          log.warn("Not yet implemented!");
          break;
      }
      if (signal) {
        signaler.signalSchedulingChange(0L);
      }
      if (jobDetail.isPersistJobDataAfterExecution() && jobDetail.getJobDataMap().isDirty()) {
        storeJob(jobDetail, true);
      }
    } catch (JobPersistenceException e) {
      log.error("Exception occurred when handling completed job " + jobDetail.getKey(), e);
    }
  }

  private boolean updateTrigger(TriggerKey key, int to, Integer from) {
    try {
      var response = client.get(g -> g.index(indexName).id(key.toString()), TriggerWrapper.class);
      if (response.found() && (from == null || response.source().getState() == from) && response.source().getState() != to) {
        var trig = response.source();
        from = trig.getState();
        trig.setState(to);
        var r = client.index(u -> u.id(key.toString()).document(trig).index(indexName));
        if (r.result() == Result.Updated) {
          log.debug("Trigger {} state updated from {} to {}", key, from, to);
          return true;
        } else {
          log.debug("Trigger {} state not updated to {}, result: {}", key, to, r.result());
          return false;
        }
      } else return false;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void setInstanceId(String schedInstId) {

  }

  @Override
  public void setInstanceName(String schedName) {

  }

  @Override
  public void setThreadPoolSize(int poolSize) {
    this.poolSize = poolSize;
  }

  @Override
  public long getAcquireRetryDelay(int failureCount) {
    return 10;
  }

  public long getMisfireThreshold() {
    return misfireThreshold;
  }

  /**
   * The the number of milliseconds by which a trigger must have missed its
   * next-fire-time, in order for it to be considered "misfired" and thus
   * have its misfire instruction applied.
   *
   * @param misfireThreshold the misfire threshold to use, in millis
   */
  @SuppressWarnings("UnusedDeclaration") /* called reflectively */
  public void setMisfireThreshold(long misfireThreshold) {
    if (misfireThreshold < 1) {
      throw new IllegalArgumentException(
          "Misfirethreshold must be larger than 0");
    }
    this.misfireThreshold = misfireThreshold;
  }

}
