using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Rhino.Util
{
    class ParallelSort
    {
//        private void QuicksortSequential<T>(T[] arr, int left, int right)
//where T : IComparable<T>
//        {
//            if (right > left)
//            {
//                int pivot = Partition(arr, left, right);
//                QuicksortSequential(arr, left, pivot - 1);
//                QuicksortSequential(arr, pivot + 1, right);
//            }
//        }

//        private void QuicksortParallelOptimised<T>(T[] arr, int left, int right)
//        where T : IComparable<T>
//        {
//            const int SEQUENTIAL_THRESHOLD = 2048;
//            if (right > left)
//            {
//                if (right - left < SEQUENTIAL_THRESHOLD)
//                {

//                    QuicksortSequential(arr, left, right);
//                }
//                else
//                {
//                    int pivot = Partition(arr, left, right);
//                    Parallel.Do(
//                        () => QuicksortParallelOptimised(arr, left, pivot - 1),
//                        () => QuicksortParallelOptimised(arr, pivot + 1, right));
//                }
//            }
//        }
    }
}
