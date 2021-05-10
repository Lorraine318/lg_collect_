// Generated from Lg.g4 by ANTLR 4.5.3

package com.lg.antlr4;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link LgParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface LgVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link LgParser#line}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLine(LgParser.LineContext ctx);
	/**
	 * Visit a parse tree produced by the {@code multOrDiv}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultOrDiv(LgParser.MultOrDivContext ctx);
	/**
	 * Visit a parse tree produced by the {@code addOrSubtract}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddOrSubtract(LgParser.AddOrSubtractContext ctx);
	/**
	 * Visit a parse tree produced by the {@code float}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloat(LgParser.FloatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenExpr}
	 * labeled alternative in {@link LgParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenExpr(LgParser.ParenExprContext ctx);
}