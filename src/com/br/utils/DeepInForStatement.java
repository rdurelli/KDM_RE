package com.br.utils;

import org.eclipse.emf.common.util.EList;
import org.eclipse.gmt.modisco.java.Block;
import org.eclipse.gmt.modisco.java.CompilationUnit;
import org.eclipse.gmt.modisco.java.ForStatement;
import org.eclipse.gmt.modisco.java.IfStatement;
import org.eclipse.gmt.modisco.java.Statement;
import org.eclipse.gmt.modisco.java.SwitchStatement;
import org.eclipse.gmt.modisco.java.TryStatement;
import org.eclipse.gmt.modisco.java.VariableDeclarationStatement;
import org.eclipse.gmt.modisco.java.WhileStatement;

public class DeepInForStatement {
	
	
	private CompilationUnit compilationUnit;
	
	private ForStatement forStatement;

	public DeepInForStatement(CompilationUnit compilationUnit,
			ForStatement forStatement) {
		super();
		this.compilationUnit = compilationUnit;
		this.forStatement = forStatement;
	}
	
	
	
	public void deep() {
		
		if (this.forStatement.getBody() != null) {
			
			if (this.forStatement.getBody() instanceof Block) {
				
				Block block = (Block) this.forStatement.getBody();
				
				EList<Statement> statements = block.getStatements();
				
				if (statements.size() > 0 || statements != null) {
					for (Statement statement : statements) {

						if (statement instanceof VariableDeclarationStatement) {

							VariableDeclarationStatement variable = (VariableDeclarationStatement) statement;

							DeepInVariableDeclarationStatement variableDeclaration = new DeepInVariableDeclarationStatement(
									compilationUnit, variable);

							variableDeclaration.deep();
						}

						else if (statement instanceof TryStatement) {

							TryStatement tryStatement = (TryStatement) statement;

							DeepInTryStatement deepInTry = new DeepInTryStatement(
									compilationUnit, tryStatement);

							deepInTry.deep();

						}

						else if (statement instanceof IfStatement) {

							IfStatement ifStatement = (IfStatement) statement;

							 DeepInIfStatement deepInIF = new
							 DeepInIfStatement(compilationUnit, ifStatement);

							 deepInIF.deep();

						}

						else if (statement instanceof ForStatement) {

							ForStatement forStatement = (ForStatement) statement;

							 DeepInForStatement deepInFOR = new
							 DeepInForStatement(compilationUnit, forStatement);

							 deepInFOR.deep();

						}

						else if (statement instanceof WhileStatement) {

							WhileStatement whileStatement = (WhileStatement) statement;

							 DeepInWhileStatement deepInWHILE = new
							 DeepInWhileStatement(compilationUnit,
							 whileStatement);

							 deepInWHILE.deep();

						}

						else if (statement instanceof SwitchStatement) {

							SwitchStatement switchStatment = (SwitchStatement) statement;

							 DeepInSwitchStatement deepInSwitch = new
							 DeepInSwitchStatement(compilationUnit,
							 switchStatment);

							 deepInSwitch.deep();

						}

					}
				}
				
			}
			
		}
		
		
		
	}

}
