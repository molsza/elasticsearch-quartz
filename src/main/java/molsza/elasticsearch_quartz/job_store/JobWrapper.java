package molsza.elasticsearch_quartz.job_store;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobWrapper
{
	private String name;
  private String description;
	private String group;
	private String jobClass;

  private String type = "job";
	private Map<String, Object> dataMap;
}
