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
import java.util.HashMap;
import java.util.Iterator;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * This method should never be used directly, use {@link SUM}.
 */
public class DoubleSum extends EvalFunc<Double> implements Algebraic, Accumulator<Double> {

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            return sum(input);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing sum in " + this.getClass().getSimpleName();
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
            // Initial is called in the map - for SUM
            // we just send the tuple down
            try {
                // input is a bag with one tuple containing
                // the column we are trying to sum
                DataBag bg = (DataBag) input.get(0);
                Double d = null;
                if(bg.iterator().hasNext()) {
                    Tuple tp = bg.iterator().next();
                    d = (Double)( tp.get(0));
                }
                return tfact.newTuple(d);
            } catch (ExecException e) {
                throw e;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing sum in " + this.getClass().getSimpleName();
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
                return tfact.newTuple(sum(input));
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing sum in " + this.getClass().getSimpleName();
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
    static public class Final extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                return sum(input);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing sum in " + this.getClass().getSimpleName();
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

    static protected Double sum(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        
        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }

        double sum = 0;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                Double d = (Double)(t.get(0));
                if (d == null) continue;
                sawNonNull = true;
                sum += d;
            }catch(RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing sum of doubles.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }
        
        if(sawNonNull) {
            return new Double(sum);
        } else {
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE)); 
    }
    
    /* Accumulator interface implementation*/
    private Double intermediateSum = null;
    
    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            Double curSum = sum(b);
            if (curSum == null) {
                return;
            }
            intermediateSum = (intermediateSum == null ? 0.0 : intermediateSum) + curSum;
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing sum in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);           
        }
    }

    @Override
    public void cleanup() {
        intermediateSum = null;
    }

    @Override
    public Double getValue() {
        return intermediateSum;
    }    
    
    /**
	 * @author iman
	 */
	@Override
	public boolean isEquivalent(EvalFunc func) {
		// TODO Auto-generated method stub
		if(func instanceof DoubleSum){
			return true;
		}
		return false;
	}
}
