/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.scripting.jython;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.scripting.ScriptEngine;
import org.python.core.Py;
import org.python.core.PyBaseCode;
import org.python.core.PyException;
import org.python.core.PyFunction;
import org.python.core.PyObject;

/**
 * Python implementation of a Pig UDF Performs mappings between Python & Pig
 * data structures
 */
public class JythonFunction extends EvalFunc<Object> {
    private PyFunction function;
    private Schema schema;
    private int num_parameters;
    private String scriptFilePath;
    private String outputSchemaFunc;
    
    public JythonFunction(String filename, String functionName) throws IOException{
        PyFunction f;
        boolean found = false;

        try {
            f = JythonScriptEngine.getFunction(filename, functionName);
            this.function = f;
            num_parameters = ((PyBaseCode) f.func_code).co_argcount;
            PyObject outputSchemaDef = f.__findattr__("outputSchema".intern());
            if (outputSchemaDef != null) {
                this.schema = Utils.getSchemaFromString(outputSchemaDef.toString());
                found = true;
            }
            PyObject outputSchemaFunctionDef = f.__findattr__("outputSchemaFunction".intern());
            if (outputSchemaFunctionDef != null) {
                if(found) {
                    throw new ExecException(
                            "multiple decorators for " + functionName);
                }
                scriptFilePath = filename;
                outputSchemaFunc = outputSchemaFunctionDef.toString();
                this.schema = null;
                found = true;
            }
            PyObject schemaFunctionDef = f.__findattr__("schemaFunction".intern());
            if (schemaFunctionDef != null) {
                if(found) {
                    throw new ExecException(
                            "multiple decorators for " + functionName);
                }
                // We should not see these functions here
                // BUG
                throw new ExecException(
                        "unregistered " + functionName);
            }
        } catch (ParseException pe) {
            throw new ExecException("Could not parse schema for script function " + pe);
        } catch (IOException e) {
            throw new IllegalStateException("Could not initialize: " + filename);
        } catch (Exception e) {
            throw new ExecException("Could not initialize: " + filename);
        }
    }

    @Override
    public Object exec(Tuple tuple) throws IOException {
        try {
            if (tuple == null || num_parameters == 0) {
                // ignore input tuple
                PyObject out = function.__call__();
                return JythonUtils.pythonToPig(out);
            }
            else {
                // this way we get the elements of the tuple as parameters instead
                // of one tuple object
                PyObject[] params = JythonUtils.pigTupleToPyTuple(tuple).getArray();
                return JythonUtils.pythonToPig(function.__call__(params));
            }
        } catch (PyException e) {
            throw new ExecException("Error executing function: " + e);
        } catch (Exception e) {
            throw new IOException("Error executing function: " + e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        if(schema != null) {
            return schema;
        } else {
            if(outputSchemaFunc != null) {
                PyFunction pf;
                try {
                    pf = JythonScriptEngine.getFunction(scriptFilePath, outputSchemaFunc);
                    // this should be a schema function
                    PyObject schemaFunctionDef = pf.__findattr__("schemaFunction".intern());
                    if(schemaFunctionDef == null) {
                        throw new IllegalStateException("Function: "
                                + outputSchemaFunc + " is not a schema function");
                    }
                    return (Schema)((pf.__call__(Py.java2py(input))).__tojava__(Object.class));
                } catch (IOException ioe) {
                    throw new IllegalStateException("Could not find function: "
                        + outputSchemaFunc + "()");
                }
            } else {
                return new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY));
            }
        }
    }
    
    @Override
	public boolean isEquivalent(EvalFunc func) {
		// TODO Auto-generated method stub
		return false;
	}
}

