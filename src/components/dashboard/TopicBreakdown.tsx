'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { AlertTriangle, BookOpen } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { motion } from 'framer-motion';

interface TopicData {
  topic: string;
  total: number;
  correct: number;
  accuracy: number;
}

interface TopicBreakdownProps {
  topics: TopicData[] | null;
  loading: boolean;
}

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.05 },
  },
};

const item = {
  hidden: { opacity: 0, x: -10 },
  show: { opacity: 1, x: 0 },
};

function getAccuracyColor(accuracy: number): string {
  if (accuracy >= 70) return 'text-emerald-500';
  if (accuracy >= 50) return 'text-yellow-500';
  return 'text-red-500';
}

function getProgressClass(accuracy: number): string {
  if (accuracy >= 70) return '[&>div]:bg-emerald-500';
  if (accuracy >= 50) return '[&>div]:bg-yellow-500';
  return '[&>div]:bg-red-500';
}

function getBgColor(accuracy: number): string {
  if (accuracy >= 70) return 'bg-emerald-50 dark:bg-emerald-950/30';
  if (accuracy >= 50) return 'bg-yellow-50 dark:bg-yellow-950/30';
  return 'bg-red-50 dark:bg-red-950/30';
}

export function TopicBreakdown({ topics, loading }: TopicBreakdownProps) {
  if (loading) {
    return (
      <div className="space-y-4">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-20 rounded-xl" />
        ))}
      </div>
    );
  }

  if (!topics || topics.length === 0) {
    return (
      <Card>
        <CardContent className="p-8 text-center">
          <BookOpen className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
          <h3 className="text-lg font-semibold mb-1">No Topic Data</h3>
          <p className="text-sm text-muted-foreground">
            Answer some quiz questions to see your topic breakdown.
          </p>
        </CardContent>
      </Card>
    );
  }

  const weakestTopic = [...topics].sort((a, b) => a.accuracy - b.accuracy)[0];
  const sortedTopics = [...topics].sort((a, b) => b.total - a.total);

  return (
    <div className="space-y-4">
      {/* Weakest Topic Highlight */}
      {weakestTopic && weakestTopic.accuracy < 50 && (
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
        >
          <Card className="border-red-200 dark:border-red-800 bg-red-50/50 dark:bg-red-950/20">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-full bg-red-100 dark:bg-red-900">
                  <AlertTriangle className="h-5 w-5 text-red-500" />
                </div>
                <div>
                  <p className="text-sm font-medium text-red-700 dark:text-red-400">
                    Weakest Topic
                  </p>
                  <p className="text-base font-bold">{weakestTopic.topic}</p>
                  <p className="text-xs text-red-600 dark:text-red-400">
                    {weakestTopic.accuracy}% accuracy ({weakestTopic.correct}/{weakestTopic.total})
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      )}

      {/* Topic List */}
      <motion.div
        variants={container}
        initial="hidden"
        animate="show"
        className="space-y-3"
      >
        {sortedTopics.map((t) => (
          <motion.div key={t.topic} variants={item}>
            <Card className={getBgColor(t.accuracy)}>
              <CardContent className="p-4">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2 min-w-0">
                    {t.accuracy < 50 && (
                      <AlertTriangle className="h-4 w-4 text-red-500 shrink-0" />
                    )}
                    <span className="text-sm font-medium truncate">{t.topic}</span>
                  </div>
                  <span className={`text-sm font-bold ${getAccuracyColor(t.accuracy)} ml-2`}>
                    {t.accuracy}%
                  </span>
                </div>
                <Progress
                  value={t.accuracy}
                  className={`h-2.5 ${getProgressClass(t.accuracy)}`}
                />
                <p className="text-xs text-muted-foreground mt-1.5">
                  {t.correct} correct out of {t.total} questions
                </p>
              </CardContent>
            </Card>
          </motion.div>
        ))}
      </motion.div>
    </div>
  );
}
