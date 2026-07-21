'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { Badge } from '@/components/ui/badge';
import { Gauge, CircleDot, Clock } from 'lucide-react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import { Skeleton } from '@/components/ui/skeleton';
import { motion } from 'framer-motion';

interface DifficultyData {
  difficulty: string;
  total: number;
  correct: number;
  accuracy: number;
  avgResponseTime: number;
}

interface DifficultyAnalysisProps {
  difficulties: DifficultyData[] | null;
  loading: boolean;
}

const DIFFICULTY_COLORS: Record<string, string> = {
  easy: '#10b981',
  medium: '#f59e0b',
  hard: '#ef4444',
  Easy: '#10b981',
  Medium: '#f59e0b',
  Hard: '#ef4444',
};

const DIFFICULTY_BG: Record<string, string> = {
  easy: 'bg-emerald-50 dark:bg-emerald-950/30',
  medium: 'bg-yellow-50 dark:bg-yellow-950/30',
  hard: 'bg-red-50 dark:bg-red-950/30',
  Easy: 'bg-emerald-50 dark:bg-emerald-950/30',
  Medium: 'bg-yellow-50 dark:bg-yellow-950/30',
  Hard: 'bg-red-50 dark:bg-red-950/30',
};

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.08 },
  },
};

const item = {
  hidden: { opacity: 0, y: 15 },
  show: { opacity: 1, y: 0 },
};

export function DifficultyAnalysis({ difficulties, loading }: DifficultyAnalysisProps) {
  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-64 rounded-xl" />
        {Array.from({ length: 3 }).map((_, i) => (
          <Skeleton key={i} className="h-24 rounded-xl" />
        ))}
      </div>
    );
  }

  if (!difficulties || difficulties.length === 0) {
    return (
      <Card>
        <CardContent className="p-8 text-center">
          <Gauge className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
          <h3 className="text-lg font-semibold mb-1">No Difficulty Data</h3>
          <p className="text-sm text-muted-foreground">
            Answer some quiz questions to see your difficulty analysis.
          </p>
        </CardContent>
      </Card>
    );
  }

  const chartData = difficulties.map((d) => ({
    name: d.difficulty,
    accuracy: d.accuracy,
    total: d.total,
    correct: d.correct,
  }));

  return (
    <motion.div
      variants={container}
      initial="hidden"
      animate="show"
      className="space-y-4"
    >
      {/* Bar Chart */}
      <motion.div variants={item}>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-base">
              <Gauge className="h-4 w-4" />
              Accuracy by Difficulty
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-48">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                  <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                  <YAxis
                    domain={[0, 100]}
                    tick={{ fontSize: 11 }}
                    tickFormatter={(val: number) => `${val}%`}
                  />
                  <Tooltip
                    formatter={(value: number) => [`${value}%`, 'Accuracy']}
                  />
                  <Bar dataKey="accuracy" radius={[4, 4, 0, 0]}>
                    {chartData.map((entry, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={DIFFICULTY_COLORS[entry.name] || '#6b7280'}
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {/* Per-Difficulty Cards */}
      {difficulties.map((d) => {
        const color = DIFFICULTY_COLORS[d.difficulty] || '#6b7280';
        const bg = DIFFICULTY_BG[d.difficulty] || '';

        return (
          <motion.div key={d.difficulty} variants={item}>
            <Card className={bg}>
              <CardContent className="p-4">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <CircleDot className="h-4 w-4" style={{ color }} />
                    <span className="font-semibold capitalize">{d.difficulty}</span>
                  </div>
                  <Badge
                    className="border-0"
                    style={{
                      backgroundColor: `${color}20`,
                      color,
                    }}
                  >
                    {d.accuracy}%
                  </Badge>
                </div>
                <Progress
                  value={d.accuracy}
                  className="h-2.5 mb-2"
                  style={
                    {
                      '--progress-color': color,
                    } as React.CSSProperties
                  }
                />
                <div className="flex items-center justify-between text-xs text-muted-foreground">
                  <span>
                    {d.correct}/{d.total} correct
                  </span>
                  <div className="flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    <span>{d.avgResponseTime}s avg</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </motion.div>
        );
      })}
    </motion.div>
  );
}
