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
package org.apache.pig.builtin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This method should never be used directly, use {@link MIN}.
 */
public class FloatMin extends EvalFunc<Float> implements Algebraic, Accumulator<Float> {

    @Override
    public Float exec(Tuple input) throws IOException {
        try {
            return min(input);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing min in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);           
        }
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                // input is a bag with one tuple containing
                // the column we are trying to min on
                DataBag bg = (DataBag) input.get(0);
                Float f = null;
                if(bg.iterator().hasNext()) {
                    Tuple tp = bg.iterator().next();
                    f = (Float)(tp.get(0));
                }
                return tfact.newTuple(f);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing min in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
        
        /**
         * @author iman
         */
        @Override
		public boolean isEquivalent(EvalFunc func) {
			// TODO Auto-generated method stub
			if(func instanceof Initial){
				return true;
			}
			return false;
		}
    }

    static public class Intermediate extends EvalFunc<Tuple> {
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(min(input));
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing min in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
        
        /**
         * @author iman
         */
        @Override
		public boolean isEquivalent(EvalFunc func) {
			// TODO Auto-generated method stub
			
			if(func instanceof Intermediate){
				return true;
			}
			return false;
		}
    }
    static public class Final extends EvalFunc<Float> {
        @Override
        public Float exec(Tuple input) throws IOException {
            try {
                return min(input);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing min in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
        
        /**
         * @author iman
         */
        @Override
		public boolean isEquivalent(EvalFunc func) {
			// TODO Auto-generated method stub
			if(func instanceof Final){
				return true;
			}
			return false;
		}
    }

    static protected Float min(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);

        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }

        float curMin = Float.POSITIVE_INFINITY;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                Float f = (Float)(t.get(0));
                if (f == null) continue;
                sawNonNull = true;
                curMin = java.lang.Math.min(curMin, f);
            } catch (RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing min of floats.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }
    
        if(sawNonNull) {
            return new Float(curMin);
        } else {
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.FLOAT)); 
    }
    
    /* Accumulator interface implementation */
    private Float intermediateMin = null;
    
    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            Float curMin = min(b);
            if (curMin == null) {
                return;
            }
            /* if bag is not null, initialize intermediateMax to negative infinity */
            if (intermediateMin == null) {
                intermediateMin = Float.POSITIVE_INFINITY;
            }
            intermediateMin = java.lang.Math.min(intermediateMin, curMin);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing min in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);           
        }
    }

    @Override
    public void cleanup() {
        intermediateMin = null;
    }

    @Override
    public Float getValue() {
        return intermediateMin;
    }
    
    /**
	 * @author iman
	 */
	@Override
	public boolean isEquivalent(EvalFunc func) {
		// TODO Auto-generated method stub
		if(func instanceof FloatMin){
			return true;
		}
		return false;
	}    
}
