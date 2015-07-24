package com.ligadata.jpmml.models;

import com.ligadata.FatafatBase.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.transform.Source;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.BiMap;
import org.apache.log4j.Logger;
import org.dmg.pmml.Entity;
import org.dmg.pmml.FieldName;
//import org.dmg.pmml.FieldValue;
import org.dmg.pmml.PMML;
import org.dmg.pmml.OpType;
import org.dmg.pmml.DataType;

import org.jpmml.evaluator.Computable;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.FieldValueUtil;
import org.jpmml.evaluator.HasProbability;
import org.jpmml.evaluator.HasEntityRegistry;
import org.jpmml.evaluator.HasEntityId;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.evaluator.TreeModelEvaluator;
import org.jpmml.evaluator.*;

import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;

import org.xml.sax.InputSource;


public class JPmmlDecisionTreeModel extends ModelBase {

	static JPmmlDecisionTreeModelObj objectSingleton = new JPmmlDecisionTreeModelObj();

	public Logger logger = Logger.getLogger(this.getClass().getName());

	public ModelResultBase execute(boolean emitAllResults) {

		// Directly calling methods from Scala Singleton object. Not preferable
		// to use direct scala.
		IrisMsg msg = (IrisMsg) modelContext().msg();
		ModelResultBase result = null;

		// Getting Java RDD Object and performing operations on that
		JavaRDDObject<IrisMsg> javaRddObj = IrisMsgFactory.toJavaRDDObject();
		Optional<IrisMsg> obj = javaRddObj.getRecent();

		if (obj.isPresent()) {
			String pmmlSrc = ModelArchive.getModelBase64(modelContext());
			PMML pmml = null;

			InputStream is = IOUtils.toInputStream(pmmlSrc);

			try {
			    Source transformedSource = ImportFilter.apply(new InputSource(is));
			    pmml = JAXBUtil.unmarshalPMML(transformedSource);


				TreeModelEvaluator modelEvaluator = new TreeModelEvaluator(pmml);

				/** The preparation of field values: */
				Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();

				List<FieldName> activeFields = modelEvaluator.getActiveFields();
				//List<FieldName> targetFields = modelEvaluator.getTargetFields();
				List<FieldName> outputFields = modelEvaluator.getOutputFields();

				Map<FieldName, FieldValue> preparedFields = prepareFields(activeFields, msg);
				// for(FieldName activeField : activeFields){
				    // The raw (ie. user-supplied) value could be any Java primitive value
				    // Object rawValue = ...;

				    // The raw value is passed through: 1) outlier treatment, 2) missing value treatment, 3) invalid value treatment and 4) type conversion
				    // FieldValue activeValue = modelEvaluator.prepare(activeField, rawValue);

				    // arguments.put(activeField, activeValue);
				// }

				/** Evaluate the model */
				Map<FieldName, ?> results = modelEvaluator.evaluate(preparedFields);

				/** get the target */
				FieldName targetName = modelEvaluator.getTargetField();
				Object targetValue = results.get(targetName);
				/** if the value is derived, get its value */
				if(targetValue instanceof Computable){
				    Computable computable = (Computable)targetValue;
				    Object primitiveValue = computable.getResult();
				    targetValue = primitiveValue;
				}

				if(targetValue instanceof HasEntityId){  /** do something with this... not sure what  */
				    HasEntityId hasEntityId = (HasEntityId)targetValue;
				    HasEntityRegistry<?> hasEntityRegistry = (HasEntityRegistry<?>)modelEvaluator;
				    BiMap<String, ? extends Entity> entities = hasEntityRegistry.getEntityRegistry();
				    Entity winner = entities.get(hasEntityId.getEntityId());

				    // Test for "probability" result feature
				    if(targetValue instanceof HasProbability){
				        HasProbability hasProbability = (HasProbability)targetValue;
				        Double winnerProbability = hasProbability.getProbability(winner.getId());
				    }
				}


		        com.ligadata.FatafatBase.Result[] returnResults = new com.ligadata.FatafatBase.Result[]{
		        	new com.ligadata.FatafatBase.Result(targetName.getValue(), targetValue)
		        };

		        logger.info("Model " + ModelName() + "'s prediction is " + targetName.getValue() + "... its value = " + targetValue.toString());

		        result = new MappedModelResults().withResults(returnResults);

		    }
		    catch (Exception e) {
		    	logger.error("Exception type : " + e.getClass().getName() + " detected... stack trace = \n" + e.getStackTrace());

			} finally {
				try {
			    	is.close();
				} catch (IOException ioe) {}
			}

		} else {
    		logger.error("the javaRddObj.getRecent() method failed to produce an rdd instance");		
		}

		return result;
	}

	public Map<FieldName, FieldValue> prepareFields(List<FieldName> activeFields, IrisMsg msg){
		Map<FieldName, FieldValue> pmmlArguments = new LinkedHashMap<FieldName, FieldValue>();

		for(FieldName activeField : activeFields) {
	    	Object userValue = msg.get(activeField.getValue());
	    	FieldValue pmmlValue = null;

	    	if (userValue instanceof String) {
	    		pmmlValue = FieldValueUtil.create(DataType.STRING, OpType.CATEGORICAL, (String)userValue);
	    	} else if (userValue instanceof Integer) {
	    		pmmlValue = FieldValueUtil.create(DataType.INTEGER, OpType.CONTINUOUS, (Integer)userValue);
	    	} else if (userValue instanceof Boolean) {
	    		pmmlValue = FieldValueUtil.create(DataType.BOOLEAN, OpType.CATEGORICAL, (boolean)userValue);
	    	} else if (userValue instanceof Float) {
	    		pmmlValue = FieldValueUtil.create(DataType.FLOAT, OpType.CONTINUOUS, (Float)userValue);
	    	} else if (userValue instanceof Double) {
	    		pmmlValue = FieldValueUtil.create(DataType.DOUBLE, OpType.CONTINUOUS, (Double)userValue);
	    	} else if (userValue instanceof Long) {

	    	} else {

	    	}
	    		
	    	if (pmmlValue != null) {
	    		pmmlArguments.put(activeField, pmmlValue);
	    	} else {
	    		// this type not handled message 
	    		logger.error("the supplied value's type " + userValue.getClass().getName() + " is not currently handled by prepareFields()");
	    		logger.error("the supplied value's string representation = " + userValue.toString() );
	    	}
	  	}
	  	return pmmlArguments;

	}


	public JPmmlDecisionTreeModel(ModelContext modelContext) {
		super(modelContext, objectSingleton);
	}

	public static class JPmmlDecisionTreeModelObj implements ModelBaseObj {
		public boolean IsValidMessage(MessageContainerBase msg) {
			return (msg instanceof IrisMsg);
		}

		public ModelBase CreateNewModel(ModelContext modelContext) {
			return new JPmmlDecisionTreeModel(modelContext);
		}

		public String ModelName() {
			return "KNIME";
		}

		public String Version() {
			return "0.0.1";
		}

		public ModelResultBase CreateResultObject() {
			return new MappedModelResults();
		}
	}
}
