using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using NLog;
using Rhino.IO;
using Rhino.Util;

namespace Rhino.MapRed
{
    class InMemoryCombineStore<InterKey, InterValue>
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        Func<List<InterValue>, InterValue> combineFunc = null;
        int minimumLengthToCombine = 4;
        
        Dictionary<InterKey, List<InterValue>> combinedDictionary = new Dictionary<InterKey, List<InterValue>>(1024);
        int intermediatePairCount = 0;
        int maxIntermediatePairsToSpill = 64 * 1024;

        TextMapperInfo mapperInfo;
        int maxIntermediateFileSize = 100 * 1024 * 1024;

        string tempDirectory;
        Guid mapperID;

        public InMemoryCombineStore(Guid mapper_id, TextMapperInfo mapper_info, string tmp_dir, Func<List<InterValue>, InterValue> combine_func = null)
        {
            mapperID = mapper_id;
            mapperInfo = mapper_info;
            tempDirectory = tmp_dir;
            combineFunc = combine_func;
        }

        public void Add(Dictionary<InterKey, List<InterValue>> dic)
        {
            foreach (var pair in dic)
            {
                List<InterValue> intermediate_list = null;
                if (!combinedDictionary.TryGetValue(pair.Key, out intermediate_list))
                {
                    intermediate_list = new List<InterValue>();
                    combinedDictionary[pair.Key] = intermediate_list;
                }

                var new_list = pair.Value;
                intermediate_list.AddRange(new_list);
                intermediatePairCount += new_list.Count;

                if (intermediate_list.Count >= minimumLengthToCombine && combineFunc != null)
                {
                    combinedDictionary[pair.Key] = new List<InterValue>() { combineFunc.Invoke(intermediate_list) };
                    intermediatePairCount -= intermediate_list.Count - 1;
                }
            }
        }

        public string doSpillIfNeeded(bool final_spill = false, int thread_num=-1)
        {
            if (combinedDictionary.Count > 0 && (intermediatePairCount + combinedDictionary.Count > maxIntermediatePairsToSpill || final_spill))
            {
                Stopwatch watch = new Stopwatch();
                watch.Restart();
                KeyValuePair<InterKey, List<InterValue>>[] sorted_pairs;
                if(thread_num<=0)
                    sorted_pairs = combinedDictionary.AsParallel().OrderBy(t => t.Key).ToArray();
                else
                    sorted_pairs = combinedDictionary.AsParallel().WithDegreeOfParallelism(thread_num).OrderBy(t => t.Key).ToArray();
                combinedDictionary.Clear();
                intermediatePairCount = 0;

                mapperInfo.SpilledRecords += sorted_pairs.Count();
                watch.Stop();
                logger.Debug("Sorted {0} records in {1}.", StringFormatter.DigitGrouped(sorted_pairs.Count()), watch.Elapsed);
                IntermediateFile<InterKey, InterValue> inter_file = new IntermediateFile<InterKey, InterValue>(tempDirectory, mapperID);
                long written_bytes = 0;
                written_bytes = inter_file.WriteRecords(sorted_pairs);
                mapperInfo.SpilledBytes += written_bytes;
                inter_file.Close();

                if (!final_spill && written_bytes > 0)
                {
                    if (written_bytes < maxIntermediateFileSize)
                    {
                        maxIntermediatePairsToSpill = (int)(maxIntermediatePairsToSpill * (double)(maxIntermediateFileSize) / written_bytes);
                        logger.Debug("maxIntermediatePairsToSpill was set to {0} records.", StringFormatter.DigitGrouped(maxIntermediatePairsToSpill));
                    }
                    if (written_bytes > 1.5 * maxIntermediateFileSize)
                    {
                        maxIntermediatePairsToSpill /= 2;
                        logger.Debug("maxIntermediatePairsToSpill was set to {0} records.", StringFormatter.DigitGrouped(maxIntermediatePairsToSpill));
                    }
                }

                return inter_file.Path;
            }
            return null;
        }



    }
}
