// Generated from Lg.g4 by ANTLR 4.5.3

package com.lg.antlr4;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link LgParser}.
 */
public interface LgListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link LgParser#line}.
	 * @param ctx the parse tree
	 */
	void enterLine(LgParser.LineContext ctx);
	/**
	 * Exit a parse tree produced by {@link LgParser#line}.
	 * @param ctx the parse tree
	 */
	void exitLine(LgParser.LineContext ctx);
	/**
	 * Enter a parse tree produced by the {@code multOrDiv}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterMultOrDiv(LgParser.MultOrDivContext ctx);
	/**
	 * Exit a parse tree produced by the {@code multOrDiv}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitMultOrDiv(LgParser.MultOrDivContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addOrSubtract}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterAddOrSubtract(LgParser.AddOrSubtractContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addOrSubtract}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitAddOrSubtract(LgParser.AddOrSubtractContext ctx);
	/**
	 * Enter a parse tree produced by the {@code float}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterFloat(LgParser.FloatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code float}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitFloat(LgParser.FloatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenExpr}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterParenExpr(LgParser.ParenExprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenExpr}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitParenExpr(LgParser.ParenExprContext ctx);
}