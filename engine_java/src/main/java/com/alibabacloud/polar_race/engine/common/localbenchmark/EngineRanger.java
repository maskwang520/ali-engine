package com.alibabacloud.polar_race.engine.common.localbenchmark;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;

import java.util.concurrent.Callable;

public class EngineRanger implements Callable<Integer>{
	public AbstractEngine engine;
	public AbstractVisitor visitor;



	public EngineRanger(AbstractEngine engine, AbstractVisitor visitor) {
		this.engine = engine;
		this.visitor = visitor;
	}


	public Integer call() throws Exception {
		engine.range(null, null, visitor);
		return 1;
	}

}
