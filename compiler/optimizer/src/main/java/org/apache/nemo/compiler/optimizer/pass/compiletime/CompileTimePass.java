/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.pass.Pass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Abstract class for compile-time optimization passes that processes the DAG.
 * It is a function that takes an original DAG to produce a processed DAG, after an optimization.
 */
public abstract class CompileTimePass extends Pass implements Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>> {
  private final Set<Class<? extends ExecutionProperty>> executionPropertiesToAnnotate;
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Constructor.
   * @param cls the pass class.
   */
  public CompileTimePass(final Class<? extends CompileTimePass> cls) {
    final Requires requires = cls.getAnnotation(Requires.class);
    this.prerequisiteExecutionProperties = requires == null
      ? new HashSet<>() : new HashSet<>(Arrays.asList(requires.value()));

    final Annotates annotates = cls.getAnnotation(Annotates.class);
    this.executionPropertiesToAnnotate = annotates == null
      ? new HashSet<>() : new HashSet<>(Arrays.asList(annotates.value()));
  }

  public Set<Class<? extends ExecutionProperty>> getExecutionPropertiesToAnnotate() {
    return executionPropertiesToAnnotate;
  }

  public Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
