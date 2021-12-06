package de.kp.works.core.cluster;

import com.google.common.base.Strings;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import de.kp.works.core.BaseConfig;
import de.kp.works.core.Params;
import de.kp.works.core.SchemaUtil;

public class PredictorConfig extends BaseConfig {

	private static final long serialVersionUID = 3691197134314567522L;

	@Description("The unique name of the clustering model that is used for prediction.")
	@Macro
	public String modelName;

	@Description("The stage of the ML model. Supported values are 'experiment', 'staging', 'production' and 'archived'. Default is 'experiment'.")
	@Macro
	public String modelStage;

	@Description(Params.MODEL_OPTION)
	@Macro
	public String modelOption;

	@Description("The name of the field in the input schema that contains the feature vector.")
	@Macro
	public String featuresCol;

	@Description("The name of the field in the output schema that contains the predicted label.")
	@Macro
	public String predictionCol;

	public PredictorConfig() {
		modelOption = BEST_MODEL;
		modelStage = "experiment";
	}

	public void validate() {

		if (Strings.isNullOrEmpty(referenceName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The reference name must not be empty.", this.getClass().getName()));
		}

		/** MODEL & COLUMNS **/
		if (Strings.isNullOrEmpty(modelName)) {
			throw new IllegalArgumentException(
					String.format("[%s] The model name must not be empty.", this.getClass().getName()));
		}
		if (Strings.isNullOrEmpty(featuresCol)) {
			throw new IllegalArgumentException(
					String.format("[%s] The name of the field that contains the feature vector must not be empty.",
							this.getClass().getName()));
		}
		if (Strings.isNullOrEmpty(predictionCol)) {
			throw new IllegalArgumentException(String.format(
					"[%s] The name of the field that contains the predicted label value must not be empty.",
					this.getClass().getName()));
		}

	}

	public void validateSchema(Schema inputSchema) {

		/** FEATURES COLUMN **/

		Schema.Field featuresField = inputSchema.getField(featuresCol);
		if (featuresField == null) {
			throw new IllegalArgumentException(
					String.format("[%s] The input schema must contain the field that defines the features.",
							this.getClass().getName()));
		}

		/** FEATURES COLUMN **/
		SchemaUtil.isArrayOfNumeric(inputSchema, featuresCol);

	}

}
