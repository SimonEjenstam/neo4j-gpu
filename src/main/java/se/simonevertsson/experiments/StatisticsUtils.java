package se.simonevertsson.experiments;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils class which contains classes to calculate means and standard deviations of runtimes, speedups etc.
 */
public class StatisticsUtils {

  static double stdDevOfGpuSpeedup(ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes, double gpuSpeedupAvg) {
    double deviationSum = 0;
    for (int i = 0; i < gpuRuntimes.size(); i++) {
      double dataPoint = (double) cypherRuntimes.get(i) / (double) gpuRuntimes.get(i);
      deviationSum += Math.pow(dataPoint - gpuSpeedupAvg, 2);
    }
    return Math.sqrt(deviationSum / (double) gpuRuntimes.size());
  }

  static double meanOfGpuSpeedup(ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes) {
    double sum = 0;
    for (int i = 0; i < gpuRuntimes.size(); i++) {
      sum += (double) cypherRuntimes.get(i) / (double) gpuRuntimes.get(i);
    }
    return sum / (double) gpuRuntimes.size();
  }

  static double stdDevOfSpeedupIncludingConversionTime(ArrayList<Long> conversionRuntimes, ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes, double conversionSpeedupAvg) {
    double deviationSum = 0;
    for (int i = 0; i < conversionRuntimes.size(); i++) {
      double dataPoint = (double) cypherRuntimes.get(i) / (double) (conversionRuntimes.get(i) + gpuRuntimes.get(i));
      deviationSum += Math.pow(dataPoint - conversionSpeedupAvg, 2);
    }
    return Math.sqrt(deviationSum / (double) conversionRuntimes.size());
  }

  static double meanOfSpeedupIncludingConversionTime(ArrayList<Long> conversionRuntimes, ArrayList<Long> gpuRuntimes, ArrayList<Long> cypherRuntimes) {
    double sum = 0;
    for (int i = 0; i < conversionRuntimes.size(); i++) {
      sum += (double) cypherRuntimes.get(i) / (double) (conversionRuntimes.get(i) + gpuRuntimes.get(i));
    }
    return sum / (double) conversionRuntimes.size();
  }

  static double stdDev(List<Long> conversionRunTimes, double conversionAvg) {
    double deviationSum = 0;
    for (long conversionRunTime : conversionRunTimes) {
      deviationSum += Math.pow((double) conversionRunTime - conversionAvg, 2);
    }
    return Math.sqrt(deviationSum / (double) conversionRunTimes.size());
  }

  static double mean(ArrayList<Long> values) {
    long sum = 0;
    for (long value : values) {
      sum += value;
    }
    return (double) sum / (double) values.size();
  }
}