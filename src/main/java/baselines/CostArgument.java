package baselines;

import btree.BPlusTree4Long;
import btree.PrimaryIndex4Time;
import common.EventSchema;
import common.IndexValuePair;
import common.Metadata;
import common.StatementParser;
import store.EventStore;
import store.RID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * ------------notice-----------------
 * intervalscan is dependend on cost model
 * so when you run this algorithm in different machines
 * you should run this class to generate cost arguments
 * and rewrite these four cost arguments
 */

public class CostArgument {


}
