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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;

public class GTOrEqualToExpr extends BinaryComparisonOperator {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    transient private final Log log = LogFactory.getLog(getClass());

    public GTOrEqualToExpr(OperatorKey k) {
        this(k, -1);
    }

    public GTOrEqualToExpr(OperatorKey k, int rp) {
        super(k, rp);
        resultType = DataType.BOOLEAN;
    }

    @Override
    public String name() {
        return "Greater Than or Equal" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitGTOrEqual(this);
    }

    @Override
    public Result getNext(Boolean bool) throws ExecException {
        byte status;
        Result left, right;

        switch (operandType) {
        case DataType.BYTEARRAY: {
            Result r = accumChild(null, dummyDBA);
            if (r != null) {
                return r;
            }
            left = lhs.getNext(dummyDBA);
            right = rhs.getNext(dummyDBA);
            return doComparison(left, right);
                            }

        case DataType.DOUBLE: {
            Result r = accumChild(null, dummyDouble);
            if (r != null) {
                return r;
            }
            left = lhs.getNext(dummyDouble);
            right = rhs.getNext(dummyDouble);
            return doComparison(left, right);
                            }

        case DataType.FLOAT: {
            Result r = accumChild(null, dummyFloat);
            if (r != null) {
                return r;
            }
            left = lhs.getNext(dummyFloat);
            right = rhs.getNext(dummyFloat);
            return doComparison(left, right);
                            }

        case DataType.INTEGER: {
            Result r = accumChild(null, dummyInt);
            if (r != null) {
                return r;
            }
            left = lhs.getNext(dummyInt);
            right = rhs.getNext(dummyInt);
            return doComparison(left, right);
                            }

        case DataType.LONG: {
            Result r = accumChild(null, dummyLong);
            if (r != null) {
                return r;
            }
            left = lhs.getNext(dummyLong);
            right = rhs.getNext(dummyLong);
            return doComparison(left, right);
                            }

        case DataType.CHARARRAY: {
            Result r = accumChild(null, dummyString);
            if (r != null) {
                return r;
            }
            left = lhs.getNext(dummyString);
            right = rhs.getNext(dummyString);
            return doComparison(left, right);
                            }


        default: {
            int errCode = 2067;
            String msg = this.getClass().getSimpleName() + " does not know how to " +
            "handle type: " + DataType.findTypeName(operandType);
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        }
    }

    @SuppressWarnings("unchecked")
    private Result doComparison(Result left, Result right) {
        if (trueRef == null) initializeRefs();
        if (left.returnStatus != POStatus.STATUS_OK) return left;
        if (right.returnStatus != POStatus.STATUS_OK) return right;
        // if either operand is null, the result should be
        // null
        if(left.result == null || right.result == null) {
            left.result = null;
            left.returnStatus = POStatus.STATUS_NULL;
            return left;
        }
        assert(left.result instanceof Comparable);
        assert(right.result instanceof Comparable);
        if (((Comparable)left.result).compareTo((Comparable)right.result) >= 0) {
            left.result = trueRef;
        } else {
            left.result = falseRef;
        }
        return left;
    }

    @Override
    public GTOrEqualToExpr clone() throws CloneNotSupportedException {
        GTOrEqualToExpr clone = new GTOrEqualToExpr(new OperatorKey(mKey.scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
        clone.cloneHelper(this);
        return clone;
    }
    
    /**
	 * @author iman
	 */
    @Override
	public boolean isEquivalent(PhysicalOperator otherOP) {
		if(otherOP instanceof GTOrEqualToExpr){
			//the other operator is also an GreaterThanOrEqual then there is a possibility of equivalence
			if(lhs.isEquivalent(((BinaryExpressionOperator) otherOP).getLhs())&& rhs.isEquivalent(((BinaryExpressionOperator) otherOP).getRhs())){
				return true;
			}
		}
		return false;
	}
}
