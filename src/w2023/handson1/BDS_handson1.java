/**
 *  Big Data Systems (Winter 2023)
 *  Hands-on 1
 *  University of New Brunswick, Fredericton
 */

package w2023.handson1;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableMap;

public class BDS_handson1 {

	private final boolean verbose = true;
	private final String SCHEMA = "dataset";
	private final String DATA_MODEL = "datasetmodel";

	Connection calConn = null;

	public static void main(String[] args) {
		new BDS_handson1().runAll();
	}

	// --------------------------------------------------------------------------------------

	/**
	 * Handson 1: using relational algebra expression implement the query:
	 * Query1: Show the name, overall (happiness) rank and score of the 5
	 * happiest countries (in ascending order of overall rank)
	 */
	private void runQuery1(RelBuilder builder) {
		System.err.println(
				"Running Q1:  Show the name, overall (happiness) rank and score of the 5 happiest countries (in ascending order of overall rank) ");
	}

	// --------------------------------------------------------------------------------------

	/**
	 * Handson 1: using relational algebra expression implement the query:
	 * Query2: For each continent, show the average happiness score.
	 */
	private void runQuery2(RelBuilder builder) {
		System.err.println("Running Q2: For each continent, show the average happiness score.");

	}

	// --------------------------------------------------------------------------------------

	/**
	 * Handson 1: using relational algebra expression implement the query:
	 * Query3: For each country with population more than 100 million, show the
	 * name of the country, population, and overall (happiness) rank.
	 */
	private void runQuery3(RelBuilder builder) {
		System.err.println(
				"Running Q3: For each country with population more than 100 million, show the name of the country, population, and overall (happiness) rank. ");

	}

	// --------------------------------------------------------------------------------------

	/**
	 * Handson 1: using relational algebra expression implement the query:
	 * Query4: For each country with life expectancy more than 80 years, show
	 * the name of the country, life expectancy, and overall (happiness) rank.
	 */
	private void runQuery4(RelBuilder builder) {
		System.err.println(
				"Running Q4: For each country with life expectancy more than 80  years, show the name of the country, life expectancy, and overall (happiness) rank. ");

	}

	// --------------------------------------------------------------------------------------

	/**
	 * Example query: Show the overall (happiness) rank of all the countries
	 */
	private void runQuery0(RelBuilder builder) {
		System.err.println("Running Q0: Show the happiness overall rank of all the countries");

		// write your relational algebra expression here
		builder.scan("country_happiness").project(builder.field("Country"), builder.field("Overall_rank"));

		// keep the following code template to build, show and execute the
		// relational algebra expression
		final RelNode node = builder.build();
		if (verbose) {
			System.out.println(RelOptUtil.toString(node));
		}

		// execute the query plan
		try {
			final PreparedStatement preparedStatement = RelRunners.run(node, calConn);
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				String country = rs.getString(1);
				int rank = rs.getInt(2);
				// System.out.println("country: " + country + " -- rank: "+
				// rank);
			}
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	// --------------------------------------------------------------------------------------

	// setting all up

	// ---------------------------------------------------------------------------------------
	public void runAll() {
		// Create a builder. The config contains a schema mapped
		final FrameworkConfig config = buildConfig();
		final RelBuilder builder = RelBuilder.create(config);

		for (int i = 0; i <= 5; i++) {
			runQueries(builder, i);
		}
	}

	// Running the examples
	private void runQueries(RelBuilder builder, int i) {
		switch (i) {
		case 0:
			runQuery0(builder);
			break;

		case 1:
			runQuery1(builder);
			break;

		case 2:
			runQuery2(builder);
			break;

		case 3:
			runQuery3(builder);
			break;

		case 4:
			runQuery4(builder);
			break;

		}
	}

	private String jsonPath(String model) {
		return resourcePath(model + ".json");
	}

	private String resourcePath(String path) {
		final URL url = BDS_handson1.class.getResource("/resources/" + path);

		String s = url.toString();
		if (s.startsWith("file:")) {
			s = s.substring("file:".length());
		}
		return s;
	}

	private FrameworkConfig buildConfig() {
		FrameworkConfig calciteFrameworkConfig = null;

		Connection connection = null;
		Statement statement = null;
		try {
			Properties info = new Properties();
			info.put("model", jsonPath(DATA_MODEL));
			connection = DriverManager.getConnection("jdbc:calcite:", info);

			final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

			calConn = calciteConnection;
			SchemaPlus rootSchemaPlus = calciteConnection.getRootSchema();

			final Schema schema = CsvSchemaFactory.INSTANCE.create(rootSchemaPlus, null,
					ImmutableMap.<String, Object> of("directory", resourcePath(SCHEMA), "flavor", "scannable"));

			SchemaPlus dbSchema = rootSchemaPlus.getSubSchema(SCHEMA);

			// Set<String> tables= schema.getTableNames();
			// for (String t: tables)
			// System.out.println(t);

			System.out.println("Available tables in the database:");
			Set<String> tables = rootSchemaPlus.getSubSchema(SCHEMA).getTableNames();
			for (String t : tables)
				System.out.println(t);

			// final SchemaPlus rootSchema = Frameworks.createRootSchema(true);

			final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

			traitDefs.add(ConventionTraitDef.INSTANCE);
			traitDefs.add(RelCollationTraitDef.INSTANCE);

			calciteFrameworkConfig = Frameworks.newConfigBuilder()
					.parserConfig(SqlParser.configBuilder()
							// Lexical configuration defines how identifiers are
							// quoted, whether they are converted to upper or
							// lower
							// case when they are read, and whether identifiers
							// are matched case-sensitively.
							.setLex(Lex.MYSQL).build())
					// Sets the schema to use by the planner
					.defaultSchema(dbSchema).traitDefs(traitDefs)
					// Context provides a way to store data within the planner
					// session that can be accessed in planner rules.
					.context(Contexts.EMPTY_CONTEXT)
					// Rule sets to use in transformation phases. Each
					// transformation phase can use a different set of rules.
					.ruleSets(RuleSets.ofList())
					// Custom cost factory to use during optimization
					.costFactory(null).typeSystem(RelDataTypeSystem.DEFAULT).build();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return calciteFrameworkConfig;
	}

}
