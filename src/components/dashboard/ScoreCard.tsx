'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import {
  Target,
  CheckCircle2,
  XCircle,
  TrendingUp,
  Flame,
  Trophy,
  Clock,
  BarChart3,
} from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';
import { Skeleton } from '@/components/ui/skeleton';
import { motion } from 'framer-motion';

interface StatsData {
  attempted: number;
  correct: number;
  wrong: number;
  accuracy: number;
  currentStreak: number;
  bestStreak: number;
  avgResponseTime: number;
  rank: number;
  totalUsers: number;
}

interface TrendData {
  daily: { date: string; total: number; correct: number; accuracy: number }[];
  weekly: { week: string; year: number; total: number; correct: number; accuracy: number }[];
}

interface ScoreCardProps {
  stats: StatsData | null;
  trend: TrendData | null;
  firstName: string;
  username: string;
  loading: boolean;
}

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.06 },
  },
};

const item = {
  hidden: { opacity: 0, y: 20 },
  show: { opacity: 1, y: 0 },
};

export function ScoreCard({ stats, trend, firstName, username, loading }: ScoreCardProps) {
  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-24 w-full rounded-xl" />
        <div className="grid grid-cols-2 gap-4">
          <Skeleton className="h-24 rounded-xl" />
          <Skeleton className="h-24 rounded-xl" />
          <Skeleton className="h-24 rounded-xl" />
          <Skeleton className="h-24 rounded-xl" />
        </div>
        <Skeleton className="h-64 rounded-xl" />
      </div>
    );
  }

  if (!stats) return null;

  const accuracyColor =
    stats.accuracy >= 70
      ? 'text-emerald-500'
      : stats.accuracy >= 50
        ? 'text-yellow-500'
        : 'text-red-500';

  const trendData = trend?.daily || [];

  return (
    <motion.div
      variants={container}
      initial="hidden"
      animate="show"
      className="space-y-4"
    >
      {/* User Profile Card */}
      <motion.div variants={item}>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-4">
              <Avatar className="h-14 w-14">
                <AvatarFallback className="bg-emerald-100 text-emerald-700 dark:bg-emerald-900 dark:text-emerald-300 text-lg font-bold">
                  {firstName ? firstName.charAt(0).toUpperCase() : '?'}
                </AvatarFallback>
              </Avatar>
              <div className="flex-1 min-w-0">
                <h2 className="text-lg font-bold truncate">{firstName || 'Unknown User'}</h2>
                {username && (
                  <p className="text-sm text-muted-foreground truncate">@{username}</p>
                )}
              </div>
              <div className="text-right">
                <div className="flex items-center gap-1">
                  <Trophy className="h-4 w-4 text-yellow-500" />
                  <span className="text-sm font-semibold">
                    #{stats.rank}
                  </span>
                </div>
                <p className="text-xs text-muted-foreground">
                  of {stats.totalUsers} users
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {/* Big Stats Grid */}
      <motion.div variants={item} className="grid grid-cols-2 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-2 mb-1">
              <Target className="h-4 w-4 text-blue-500" />
              <span className="text-xs font-medium text-muted-foreground">Attempted</span>
            </div>
            <p className="text-2xl font-bold">{stats.attempted}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-2 mb-1">
              <CheckCircle2 className="h-4 w-4 text-emerald-500" />
              <span className="text-xs font-medium text-muted-foreground">Correct</span>
            </div>
            <p className="text-2xl font-bold text-emerald-500">{stats.correct}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-2 mb-1">
              <XCircle className="h-4 w-4 text-red-500" />
              <span className="text-xs font-medium text-muted-foreground">Wrong</span>
            </div>
            <p className="text-2xl font-bold text-red-500">{stats.wrong}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-2 mb-1">
              <TrendingUp className="h-4 w-4 text-blue-500" />
              <span className="text-xs font-medium text-muted-foreground">Accuracy</span>
            </div>
            <p className={`text-2xl font-bold ${accuracyColor}`}>
              {stats.accuracy}%
            </p>
          </CardContent>
        </Card>
      </motion.div>

      {/* Streaks & Response Time */}
      <motion.div variants={item} className="grid grid-cols-3 gap-3">
        <Card>
          <CardContent className="p-3 text-center">
            <Flame className="h-5 w-5 mx-auto mb-1 text-orange-500" />
            <p className="text-xs text-muted-foreground">Current</p>
            <p className="text-xl font-bold">{stats.currentStreak}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-3 text-center">
            <Flame className="h-5 w-5 mx-auto mb-1 text-red-500" />
            <p className="text-xs text-muted-foreground">Best</p>
            <p className="text-xl font-bold">{stats.bestStreak}</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-3 text-center">
            <Clock className="h-5 w-5 mx-auto mb-1 text-blue-500" />
            <p className="text-xs text-muted-foreground">Avg Time</p>
            <p className="text-xl font-bold">{stats.avgResponseTime}s</p>
          </CardContent>
        </Card>
      </motion.div>

      {/* Accuracy Bar */}
      <motion.div variants={item}>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium">Overall Accuracy</span>
              <Badge
                variant={stats.accuracy >= 70 ? 'default' : 'secondary'}
                className={
                  stats.accuracy >= 70
                    ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900 dark:text-emerald-300'
                    : stats.accuracy >= 50
                      ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300'
                      : 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'
                }
              >
                {stats.accuracy}%
              </Badge>
            </div>
            <Progress
              value={stats.accuracy}
              className="h-3"
            />
          </CardContent>
        </Card>
      </motion.div>

      {/* Accuracy Trend Chart */}
      <motion.div variants={item}>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-base">
              <BarChart3 className="h-4 w-4" />
              Accuracy Trend (Last 30 Days)
            </CardTitle>
          </CardHeader>
          <CardContent>
            {trendData.length > 0 ? (
              <div className="h-56">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={trendData}>
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
            ) : (
              <div className="h-56 flex items-center justify-center text-muted-foreground text-sm">
                No trend data available yet
              </div>
            )}
          </CardContent>
        </Card>
      </motion.div>
    </motion.div>
  );
}
