'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  ReferenceLine,
} from 'recharts';
import { Skeleton } from '@/components/ui/skeleton';
import { motion } from 'framer-motion';

interface TrendData {
  daily: { date: string; total: number; correct: number; accuracy: number }[];
  weekly: { week: string; year: number; total: number; correct: number; accuracy: number }[];
}

interface ImprovementTrendProps {
  trend: TrendData | null;
  loading: boolean;
}

export function ImprovementTrend({ trend, loading }: ImprovementTrendProps) {
  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-64 rounded-xl" />
        <Skeleton className="h-64 rounded-xl" />
      </div>
    );
  }

  if (!trend || (trend.daily.length === 0 && trend.weekly.length === 0)) {
    return (
      <Card>
        <CardContent className="p-8 text-center">
          <TrendingUp className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
          <h3 className="text-lg font-semibold mb-1">No Trend Data</h3>
          <p className="text-sm text-muted-foreground">
            Answer quiz questions over multiple days to see your improvement trend.
          </p>
        </CardContent>
      </Card>
    );
  }

  const dailyData = trend.daily;
  const weeklyData = trend.weekly;

  // Calculate trend direction
  let trendDirection: 'up' | 'down' | 'stable' = 'stable';
  if (dailyData.length >= 2) {
    const recentAvg = dailyData.slice(-7).reduce((sum, d) => sum + d.accuracy, 0) / Math.min(7, dailyData.slice(-7).length);
    const olderAvg = dailyData.slice(0, 7).reduce((sum, d) => sum + d.accuracy, 0) / Math.min(7, dailyData.slice(0, 7).length);
    if (recentAvg > olderAvg + 5) trendDirection = 'up';
    else if (recentAvg < olderAvg - 5) trendDirection = 'down';
  }

  const TrendIcon =
    trendDirection === 'up' ? TrendingUp : trendDirection === 'down' ? TrendingDown : Minus;
  const trendColor =
    trendDirection === 'up'
      ? 'text-emerald-500'
      : trendDirection === 'down'
        ? 'text-red-500'
        : 'text-yellow-500';
  const trendLabel =
    trendDirection === 'up'
      ? 'Improving'
      : trendDirection === 'down'
        ? 'Declining'
        : 'Stable';

  return (
    <div className="space-y-4">
      {/* Trend Summary */}
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
      >
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-full ${
                trendDirection === 'up'
                  ? 'bg-emerald-100 dark:bg-emerald-900'
                  : trendDirection === 'down'
                    ? 'bg-red-100 dark:bg-red-900'
                    : 'bg-yellow-100 dark:bg-yellow-900'
              }`}>
                <TrendIcon className={`h-5 w-5 ${trendColor}`} />
              </div>
              <div>
                <p className="text-sm text-muted-foreground">Overall Trend</p>
                <p className={`text-lg font-bold ${trendColor}`}>{trendLabel}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {/* Daily Accuracy Line Chart */}
      {dailyData.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
        >
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Daily Accuracy</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-56">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={dailyData}>
                    <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                    <XAxis
                      dataKey="date"
                      tick={{ fontSize: 11 }}
                      tickFormatter={(val: string) => {
                        const parts = val.split('-');
                        return `${parts[1]}/${parts[2]}`;
                      }}
                    />
                    <YAxis
                      domain={[0, 100]}
                      tick={{ fontSize: 11 }}
                      tickFormatter={(val: number) => `${val}%`}
                    />
                    <Tooltip
                      formatter={(value: number) => [`${value}%`, 'Accuracy']}
                      labelFormatter={(label: string) => `Date: ${label}`}
                    />
                    <ReferenceLine y={70} stroke="#10b981" strokeDasharray="5 5" opacity={0.5} />
                    <ReferenceLine y={50} stroke="#f59e0b" strokeDasharray="5 5" opacity={0.3} />
                    <Line
                      type="monotone"
                      dataKey="accuracy"
                      stroke="#10b981"
                      strokeWidth={2}
                      dot={{ r: 3, fill: '#10b981' }}
                      activeDot={{ r: 5 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      )}

      {/* Weekly Comparison Bar Chart */}
      {weeklyData.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
        >
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Weekly Comparison</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-56">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={weeklyData}>
                    <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                    <XAxis dataKey="week" tick={{ fontSize: 11 }} />
                    <YAxis
                      domain={[0, 100]}
                      tick={{ fontSize: 11 }}
                      tickFormatter={(val: number) => `${val}%`}
                    />
                    <Tooltip
                      formatter={(value: number) => [`${value}%`, 'Accuracy']}
                    />
                    <ReferenceLine y={70} stroke="#10b981" strokeDasharray="5 5" opacity={0.5} />
                    <Bar dataKey="accuracy" radius={[4, 4, 0, 0]} fill="#10b981" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      )}

      {/* Questions per Day */}
      {dailyData.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
        >
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-base">Activity (Questions/Day)</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-40">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={dailyData}>
                    <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                    <XAxis
                      dataKey="date"
                      tick={{ fontSize: 10 }}
                      tickFormatter={(val: string) => {
                        const parts = val.split('-');
                        return `${parts[1]}/${parts[2]}`;
                      }}
                    />
                    <YAxis tick={{ fontSize: 11 }} />
                    <Tooltip
                      formatter={(value: number) => [value, 'Questions']}
                    />
                    <Bar dataKey="total" radius={[3, 3, 0, 0]} fill="#6b7280" opacity={0.4} />
                    <Bar dataKey="correct" radius={[3, 3, 0, 0]} fill="#10b981" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      )}
    </div>
  );
}
