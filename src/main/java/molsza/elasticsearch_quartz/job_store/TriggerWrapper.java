package molsza.elasticsearch_quartz.job_store;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents a wrapped trigger.
 *
 * @author Anton Johansson
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class TriggerWrapper {
  public static final transient int STATE_WAITING = 0;
  public static final transient int STATE_ACQUIRED = 1;
  public static final transient int STATE_EXECUTING = 2;
  public static final transient int STATE_COMPLETED = 3;
  public static final transient int STATE_PAUSE = 6;
  public static final transient int STATE_ERROR = 7;

  private String name;
  private String group;
  private String triggerClass;
  private String jobName;
  private String jobGroup;
  private int state;
  private long startTime;
  private long endTime;
  private long nextFireTime;
  private long previousFireTime;
  private long executionTimeout;
  private long maxExecutionTime;
  private int priority;
  private int repeatCount;
  private long repeatInterval;
  private int timesTriggered;
  private String cronExpression;

  private String type = "trigger";
}
