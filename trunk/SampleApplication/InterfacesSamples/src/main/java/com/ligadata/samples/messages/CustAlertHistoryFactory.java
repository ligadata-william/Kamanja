package com.ligadata.samples.messages;

import com.google.common.base.Optional;
import com.ligadata.FatafatBase.BaseMsg;
import com.ligadata.FatafatBase.BaseContainer;
import com.ligadata.FatafatBase.InputData;
import com.ligadata.FatafatBase.JavaRDD;
import com.ligadata.FatafatBase.JavaRDDObject;
import com.ligadata.FatafatBase.RDDObject;
import com.google.common.base.Optional;
import com.ligadata.Utils.Utils;
import com.ligadata.FatafatBase.TimeRange;
import com.ligadata.FatafatBase.MessageContainerBase;
import com.ligadata.FatafatBase.api.java.function.Function1;
import com.ligadata.FatafatBase.FatafatUtils;

public final class CustAlertHistoryFactory {
	public static String FullName() {
		return CustAlertHistory$.MODULE$.FullName();
	}

	public static String NameSpace() {
		return CustAlertHistory$.MODULE$.NameSpace();
	}

	public static String Name() {
		return CustAlertHistory$.MODULE$.Name();
	}
	
	public static String Version() {
		return CustAlertHistory$.MODULE$.Version();
	}

	public static BaseContainer CreateNewContainer() {
		return CustAlertHistory$.MODULE$.CreateNewContainer();
	}
	
	public static boolean IsFixed() {
		return CustAlertHistory$.MODULE$.IsFixed();
	}
	
	public static boolean IsKv() {
		return CustAlertHistory$.MODULE$.IsKv();
	}
	
	public static boolean CanPersist() {
		return CustAlertHistory$.MODULE$.CanPersist();
	}
	
	public static CustAlertHistory build() {
		return CustAlertHistory$.MODULE$.build();
	}

	public static CustAlertHistory build(CustAlertHistory from) {
		return CustAlertHistory$.MODULE$.build(from);
	}
	
	public static String[] PartitionKeyData(InputData inputdata) {
		return CustAlertHistory$.MODULE$.PartitionKeyData(inputdata);
	}
	
	public static String[] PrimaryKeyData(InputData inputdata) {
		return CustAlertHistory$.MODULE$.PrimaryKeyData(inputdata);
	}

	public static String getFullName() {
		return CustAlertHistory$.MODULE$.getFullName();
	}
	
	public static JavaRDDObject<CustAlertHistory> toJavaRDDObject() {
		return CustAlertHistory$.MODULE$.toJavaRDDObject();
	}

	public static RDDObject<CustAlertHistory> toRDDObject() {
		return CustAlertHistory$.MODULE$.toRDDObject();
	}

	public static Optional<CustAlertHistory> getRecent() {
		return  Utils.optionToOptional(CustAlertHistory$.MODULE$.getRecent());
	}

	public static CustAlertHistory getRecentOrNew() {
		return CustAlertHistory$.MODULE$.getRecentOrNew();
	}

	public static Optional<CustAlertHistory> getRecent(String[] key) {
		return Utils.optionToOptional(CustAlertHistory$.MODULE$.getRecent(key));
	}

	public static CustAlertHistory getRecentOrNew(String[] key) {
		return CustAlertHistory$.MODULE$.getRecentOrNew(key);
	}
	
/*
	public static Optional<CustAlertHistory> getOne(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return Utils.optionToOptional(CustAlertHistory$.MODULE$.getOne(tmRange, FatafatUtils.toScalaFunction1(f)));
	}

	public static CustAlertHistory getOneOrNew(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getOneOrNew(tmRange, FatafatUtils.toScalaFunction1(f));
	}

	public static Optional<CustAlertHistory> getOne(String[] key, TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return Utils.optionToOptional(CustAlertHistory$.MODULE$.getOne(key, tmRange, FatafatUtils.toScalaFunction1(f)));
	}

	public static CustAlertHistory getOneOrNew(String[] key, TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getOneOrNew(key, tmRange, FatafatUtils.toScalaFunction1(f));
	}

	public static JavaRDD<CustAlertHistory> getRDDForCurrKey(Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getRDDForCurrKey(FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}

	public static JavaRDD<CustAlertHistory> getRDDForCurrKey(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getRDDForCurrKey(tmRange, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}
	
	public static JavaRDD<CustAlertHistory> getRDD(TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getRDD(tmRange, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}
	
*/
	public static JavaRDD<CustAlertHistory> getRDD(TimeRange tmRange) {
		return CustAlertHistory$.MODULE$.getRDD(tmRange).toJavaRDD();
	}
	
/*
	public static JavaRDD<CustAlertHistory> getRDD(Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getRDD(FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}

	public static JavaRDD<CustAlertHistory> getRDD(String[] key, TimeRange tmRange, Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getRDD(key, tmRange, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}
	
	public static JavaRDD<CustAlertHistory> getRDD(String[] key, Function1<MessageContainerBase, Boolean> f) {
		return CustAlertHistory$.MODULE$.getRDD(key, FatafatUtils.toScalaFunction1(f)).toJavaRDD();
	}

*/

	public static JavaRDD<CustAlertHistory> getRDD(String[] key, TimeRange tmRange) {
		return CustAlertHistory$.MODULE$.getRDD(key, tmRange).toJavaRDD();
	}
	
	public static void saveOne(CustAlertHistory inst) {
		CustAlertHistory$.MODULE$.saveOne(inst);
	}
	
	public static void saveOne(String[] key, CustAlertHistory inst) {
		CustAlertHistory$.MODULE$.saveOne(key, inst);
	}
	
	public static void saveRDD(JavaRDD<CustAlertHistory> data) {
		CustAlertHistory$.MODULE$.saveRDD(data.rdd());
	}
}

