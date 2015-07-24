package com.ligadata.jpmml.models;

import com.ligadata.FatafatBase.*;
import com.google.common.base.Optional;
import org.apache.commons.codec.binary.Base64;

class ModelArchive {

    /**
        Obtain the model from the model context.  

        NOTE: The model context could conceivably contain the model source, but it is really not
        needed or for some model types may not even be available (jar only model submission).
        We might consider using key information (a hash of PMML perhaps) here that is used to 
        fetch the model source from the peristent store, or alternatively only supply source when 
        the jpmml model type or other models of its ilk are in use.

        @param mdlContext a ModelContext with the necessary information to obtain the PMML source
        @return the model associated with this ModelContext
     */
    static String getModel(ModelContext mdlContext) {
        // return mdlContext.modelSource
        return irisDecisionTree;
    }

    static String irisDecisionTree = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<PMML version=\"4.1\" xmlns=\"http://www.dmg.org/PMML-4_1\">" +
        "  <Header copyright=\"KNIME\">" +
        "    <Application name=\"KNIME\" version=\"2.8.0\"/>" +
        "  </Header>" +
        "  <DataDictionary numberOfFields=\"5\">" +
        "    <DataField name=\"sepal_length\" optype=\"continuous\" dataType=\"double\">" +
        "      <Interval closure=\"closedClosed\" leftMargin=\"4.3\" rightMargin=\"7.9\"/>" +
        "    </DataField>" +
        "    <DataField name=\"sepal_width\" optype=\"continuous\" dataType=\"double\">" +
        "      <Interval closure=\"closedClosed\" leftMargin=\"2.0\" rightMargin=\"4.4\"/>" +
        "    </DataField>" +
        "    <DataField name=\"petal_length\" optype=\"continuous\" dataType=\"double\">" +
        "      <Interval closure=\"closedClosed\" leftMargin=\"1.0\" rightMargin=\"6.9\"/>" +
        "    </DataField>" +
        "    <DataField name=\"petal_width\" optype=\"continuous\" dataType=\"double\">" +
        "      <Interval closure=\"closedClosed\" leftMargin=\"0.1\" rightMargin=\"2.5\"/>" +
        "    </DataField>" +
        "    <DataField name=\"class\" optype=\"categorical\" dataType=\"string\">" +
        "      <Value value=\"Iris-setosa\"/>" +
        "      <Value value=\"Iris-versicolor\"/>" +
        "      <Value value=\"Iris-virginica\"/>" +
        "    </DataField>" +
        "  </DataDictionary>" +
        "  <TreeModel modelName=\"DecisionTree\" functionName=\"classification\" splitCharacteristic=\"binarySplit\" missingValueStrategy=\"lastPrediction\" noTrueChildStrategy=\"returnNullPrediction\">" +
        "    <MiningSchema>" +
        "      <MiningField name=\"sepal_length\" invalidValueTreatment=\"asIs\"/>" +
        "      <MiningField name=\"sepal_width\" invalidValueTreatment=\"asIs\"/>" +
        "      <MiningField name=\"petal_length\" invalidValueTreatment=\"asIs\"/>" +
        "      <MiningField name=\"petal_width\" invalidValueTreatment=\"asIs\"/>" +
        "      <MiningField name=\"class\" invalidValueTreatment=\"asIs\" usageType=\"predicted\"/>" +
        "    </MiningSchema>" +
        "    <Node id=\"0\" score=\"Iris-setosa\" recordCount=\"150.0\">" +
        "      <True/>" +
        "      <ScoreDistribution value=\"Iris-setosa\" recordCount=\"50.0\"/>" +
        "      <ScoreDistribution value=\"Iris-versicolor\" recordCount=\"50.0\"/>" +
        "      <ScoreDistribution value=\"Iris-virginica\" recordCount=\"50.0\"/>" +
        "      <Node id=\"1\" score=\"Iris-setosa\" recordCount=\"50.0\">" +
        "        <SimplePredicate field=\"petal_width\" operator=\"lessOrEqual\" value=\"0.6\"/>" +
        "        <ScoreDistribution value=\"Iris-setosa\" recordCount=\"50.0\"/>" +
        "        <ScoreDistribution value=\"Iris-versicolor\" recordCount=\"0.0\"/>" +
        "        <ScoreDistribution value=\"Iris-virginica\" recordCount=\"0.0\"/>" +
        "      </Node>" +
        "      <Node id=\"2\" score=\"Iris-versicolor\" recordCount=\"100.0\">" +
        "        <SimplePredicate field=\"petal_width\" operator=\"greaterThan\" value=\"0.6\"/>" +
        "        <ScoreDistribution value=\"Iris-setosa\" recordCount=\"0.0\"/>" +
        "        <ScoreDistribution value=\"Iris-versicolor\" recordCount=\"50.0\"/>" +
        "        <ScoreDistribution value=\"Iris-virginica\" recordCount=\"50.0\"/>" +
        "        <Node id=\"3\" score=\"Iris-versicolor\" recordCount=\"54.0\">" +
        "          <SimplePredicate field=\"petal_width\" operator=\"lessOrEqual\" value=\"1.7\"/>" +
        "          <ScoreDistribution value=\"Iris-setosa\" recordCount=\"0.0\"/>" +
        "          <ScoreDistribution value=\"Iris-versicolor\" recordCount=\"49.0\"/>" +
        "          <ScoreDistribution value=\"Iris-virginica\" recordCount=\"5.0\"/>" +
        "        </Node>" +
        "        <Node id=\"10\" score=\"Iris-virginica\" recordCount=\"46.0\">" +
        "          <SimplePredicate field=\"petal_width\" operator=\"greaterThan\" value=\"1.7\"/>" +
        "          <ScoreDistribution value=\"Iris-setosa\" recordCount=\"0.0\"/>" +
        "          <ScoreDistribution value=\"Iris-versicolor\" recordCount=\"1.0\"/>" +
        "          <ScoreDistribution value=\"Iris-virginica\" recordCount=\"45.0\"/>" +
        "        </Node>" +
        "      </Node>" +
        "    </Node>" +
        "  </TreeModel>" +
        "</PMML>" +
        "";

    static String getModelBase64(ModelContext mdlContext) {
        // return mdlContext.modelSource
        Base64 decoder = new Base64();
        byte[] decodedBytes = decoder.decode(irisDecisionTreeBase64);
        String modelPmml = new String(decodedBytes);
        return modelPmml;
    }


    static String irisDecisionTreeBase64 = "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPFBNTUwgdmVyc2lvbj0iNC4xIiB4bWxucz0iaHR0cDovL3d3dy5kbWcub3JnL1BNTUwtNF8xIj4KICA8SGVhZGVyIGNvcHlyaWdodD0iS05JTUUiPgogICAgPEFwcGxpY2F0aW9uIG5hbWU9IktOSU1FIiB2ZXJzaW9uPSIyLjguMCIvPgogIDwvSGVhZGVyPgogIDxEYXRhRGljdGlvbmFyeSBudW1iZXJPZkZpZWxkcz0iNSI+CiAgICA8RGF0YUZpZWxkIG5hbWU9InNlcGFsX2xlbmd0aCIgb3B0eXBlPSJjb250aW51b3VzIiBkYXRhVHlwZT0iZG91YmxlIj4KICAgICAgPEludGVydmFsIGNsb3N1cmU9ImNsb3NlZENsb3NlZCIgbGVmdE1hcmdpbj0iNC4zIiByaWdodE1hcmdpbj0iNy45Ii8+CiAgICA8L0RhdGFGaWVsZD4KICAgIDxEYXRhRmllbGQgbmFtZT0ic2VwYWxfd2lkdGgiIG9wdHlwZT0iY29udGludW91cyIgZGF0YVR5cGU9ImRvdWJsZSI+CiAgICAgIDxJbnRlcnZhbCBjbG9zdXJlPSJjbG9zZWRDbG9zZWQiIGxlZnRNYXJnaW49IjIuMCIgcmlnaHRNYXJnaW49IjQuNCIvPgogICAgPC9EYXRhRmllbGQ+CiAgICA8RGF0YUZpZWxkIG5hbWU9InBldGFsX2xlbmd0aCIgb3B0eXBlPSJjb250aW51b3VzIiBkYXRhVHlwZT0iZG91YmxlIj4KICAgICAgPEludGVydmFsIGNsb3N1cmU9ImNsb3NlZENsb3NlZCIgbGVmdE1hcmdpbj0iMS4wIiByaWdodE1hcmdpbj0iNi45Ii8+CiAgICA8L0RhdGFGaWVsZD4KICAgIDxEYXRhRmllbGQgbmFtZT0icGV0YWxfd2lkdGgiIG9wdHlwZT0iY29udGludW91cyIgZGF0YVR5cGU9ImRvdWJsZSI+CiAgICAgIDxJbnRlcnZhbCBjbG9zdXJlPSJjbG9zZWRDbG9zZWQiIGxlZnRNYXJnaW49IjAuMSIgcmlnaHRNYXJnaW49IjIuNSIvPgogICAgPC9EYXRhRmllbGQ+CiAgICA8RGF0YUZpZWxkIG5hbWU9ImNsYXNzIiBvcHR5cGU9ImNhdGVnb3JpY2FsIiBkYXRhVHlwZT0ic3RyaW5nIj4KICAgICAgPFZhbHVlIHZhbHVlPSJJcmlzLXNldG9zYSIvPgogICAgICA8VmFsdWUgdmFsdWU9IklyaXMtdmVyc2ljb2xvciIvPgogICAgICA8VmFsdWUgdmFsdWU9IklyaXMtdmlyZ2luaWNhIi8+CiAgICA8L0RhdGFGaWVsZD4KICA8L0RhdGFEaWN0aW9uYXJ5PgogIDxUcmVlTW9kZWwgbW9kZWxOYW1lPSJEZWNpc2lvblRyZWUiIGZ1bmN0aW9uTmFtZT0iY2xhc3NpZmljYXRpb24iIHNwbGl0Q2hhcmFjdGVyaXN0aWM9ImJpbmFyeVNwbGl0IiBtaXNzaW5nVmFsdWVTdHJhdGVneT0ibGFzdFByZWRpY3Rpb24iIG5vVHJ1ZUNoaWxkU3RyYXRlZ3k9InJldHVybk51bGxQcmVkaWN0aW9uIj4KICAgIDxNaW5pbmdTY2hlbWE+CiAgICAgIDxNaW5pbmdGaWVsZCBuYW1lPSJzZXBhbF9sZW5ndGgiIGludmFsaWRWYWx1ZVRyZWF0bWVudD0iYXNJcyIvPgogICAgICA8TWluaW5nRmllbGQgbmFtZT0ic2VwYWxfd2lkdGgiIGludmFsaWRWYWx1ZVRyZWF0bWVudD0iYXNJcyIvPgogICAgICA8TWluaW5nRmllbGQgbmFtZT0icGV0YWxfbGVuZ3RoIiBpbnZhbGlkVmFsdWVUcmVhdG1lbnQ9ImFzSXMiLz4KICAgICAgPE1pbmluZ0ZpZWxkIG5hbWU9InBldGFsX3dpZHRoIiBpbnZhbGlkVmFsdWVUcmVhdG1lbnQ9ImFzSXMiLz4KICAgICAgPE1pbmluZ0ZpZWxkIG5hbWU9ImNsYXNzIiBpbnZhbGlkVmFsdWVUcmVhdG1lbnQ9ImFzSXMiIHVzYWdlVHlwZT0icHJlZGljdGVkIi8+CiAgICA8L01pbmluZ1NjaGVtYT4KICAgIDxOb2RlIGlkPSIwIiBzY29yZT0iSXJpcy1zZXRvc2EiIHJlY29yZENvdW50PSIxNTAuMCI+CiAgICAgIDxUcnVlLz4KICAgICAgPFNjb3JlRGlzdHJpYnV0aW9uIHZhbHVlPSJJcmlzLXNldG9zYSIgcmVjb3JkQ291bnQ9IjUwLjAiLz4KICAgICAgPFNjb3JlRGlzdHJpYnV0aW9uIHZhbHVlPSJJcmlzLXZlcnNpY29sb3IiIHJlY29yZENvdW50PSI1MC4wIi8+CiAgICAgIDxTY29yZURpc3RyaWJ1dGlvbiB2YWx1ZT0iSXJpcy12aXJnaW5pY2EiIHJlY29yZENvdW50PSI1MC4wIi8+CiAgICAgIDxOb2RlIGlkPSIxIiBzY29yZT0iSXJpcy1zZXRvc2EiIHJlY29yZENvdW50PSI1MC4wIj4KICAgICAgICA8U2ltcGxlUHJlZGljYXRlIGZpZWxkPSJwZXRhbF93aWR0aCIgb3BlcmF0b3I9Imxlc3NPckVxdWFsIiB2YWx1ZT0iMC42Ii8+CiAgICAgICAgPFNjb3JlRGlzdHJpYnV0aW9uIHZhbHVlPSJJcmlzLXNldG9zYSIgcmVjb3JkQ291bnQ9IjUwLjAiLz4KICAgICAgICA8U2NvcmVEaXN0cmlidXRpb24gdmFsdWU9IklyaXMtdmVyc2ljb2xvciIgcmVjb3JkQ291bnQ9IjAuMCIvPgogICAgICAgIDxTY29yZURpc3RyaWJ1dGlvbiB2YWx1ZT0iSXJpcy12aXJnaW5pY2EiIHJlY29yZENvdW50PSIwLjAiLz4KICAgICAgPC9Ob2RlPgogICAgICA8Tm9kZSBpZD0iMiIgc2NvcmU9IklyaXMtdmVyc2ljb2xvciIgcmVjb3JkQ291bnQ9IjEwMC4wIj4KICAgICAgICA8U2ltcGxlUHJlZGljYXRlIGZpZWxkPSJwZXRhbF93aWR0aCIgb3BlcmF0b3I9ImdyZWF0ZXJUaGFuIiB2YWx1ZT0iMC42Ii8+CiAgICAgICAgPFNjb3JlRGlzdHJpYnV0aW9uIHZhbHVlPSJJcmlzLXNldG9zYSIgcmVjb3JkQ291bnQ9IjAuMCIvPgogICAgICAgIDxTY29yZURpc3RyaWJ1dGlvbiB2YWx1ZT0iSXJpcy12ZXJzaWNvbG9yIiByZWNvcmRDb3VudD0iNTAuMCIvPgogICAgICAgIDxTY29yZURpc3RyaWJ1dGlvbiB2YWx1ZT0iSXJpcy12aXJnaW5pY2EiIHJlY29yZENvdW50PSI1MC4wIi8+CiAgICAgICAgPE5vZGUgaWQ9IjMiIHNjb3JlPSJJcmlzLXZlcnNpY29sb3IiIHJlY29yZENvdW50PSI1NC4wIj4KICAgICAgICAgIDxTaW1wbGVQcmVkaWNhdGUgZmllbGQ9InBldGFsX3dpZHRoIiBvcGVyYXRvcj0ibGVzc09yRXF1YWwiIHZhbHVlPSIxLjciLz4KICAgICAgICAgIDxTY29yZURpc3RyaWJ1dGlvbiB2YWx1ZT0iSXJpcy1zZXRvc2EiIHJlY29yZENvdW50PSIwLjAiLz4KICAgICAgICAgIDxTY29yZURpc3RyaWJ1dGlvbiB2YWx1ZT0iSXJpcy12ZXJzaWNvbG9yIiByZWNvcmRDb3VudD0iNDkuMCIvPgogICAgICAgICAgPFNjb3JlRGlzdHJpYnV0aW9uIHZhbHVlPSJJcmlzLXZpcmdpbmljYSIgcmVjb3JkQ291bnQ9IjUuMCIvPgogICAgICAgIDwvTm9kZT4KICAgICAgICA8Tm9kZSBpZD0iMTAiIHNjb3JlPSJJcmlzLXZpcmdpbmljYSIgcmVjb3JkQ291bnQ9IjQ2LjAiPgogICAgICAgICAgPFNpbXBsZVByZWRpY2F0ZSBmaWVsZD0icGV0YWxfd2lkdGgiIG9wZXJhdG9yPSJncmVhdGVyVGhhbiIgdmFsdWU9IjEuNyIvPgogICAgICAgICAgPFNjb3JlRGlzdHJpYnV0aW9uIHZhbHVlPSJJcmlzLXNldG9zYSIgcmVjb3JkQ291bnQ9IjAuMCIvPgogICAgICAgICAgPFNjb3JlRGlzdHJpYnV0aW9uIHZhbHVlPSJJcmlzLXZlcnNpY29sb3IiIHJlY29yZENvdW50PSIxLjAiLz4KICAgICAgICAgIDxTY29yZURpc3RyaWJ1dGlvbiB2YWx1ZT0iSXJpcy12aXJnaW5pY2EiIHJlY29yZENvdW50PSI0NS4wIi8+CiAgICAgICAgPC9Ob2RlPgogICAgICA8L05vZGU+CiAgICA8L05vZGU+CiAgPC9UcmVlTW9kZWw+CjwvUE1NTD4=";

}