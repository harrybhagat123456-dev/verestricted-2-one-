'use client';

import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';
import { ScrollArea } from '@/components/ui/scroll-area';
import { XCircle, CheckCircle2, RotateCcw, Lightbulb, ExternalLink } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { motion } from 'framer-motion';
import { useState } from 'react';

interface WrongQuestion {
  hq_id: string;
  question: string;
  options: string[];
  yourAnswer: string;
  correctAnswer: string;
  selected_option: number;
  correct_option: number;
  explanation: {
    text: string;
    images: string[];
    videos: string[];
    photos: string[];
    kind: string;
  } | null;
  answered_at: string;
}

interface WrongQuestionsProps {
  wrongQuestions: WrongQuestion[] | null;
  loading: boolean;
}

export function WrongQuestions({ wrongQuestions, loading }: WrongQuestionsProps) {
  const [expandedItems, setExpandedItems] = useState<string[]>([]);

  if (loading) {
    return (
      <div className="space-y-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-32 rounded-xl" />
        ))}
      </div>
    );
  }

  if (!wrongQuestions || wrongQuestions.length === 0) {
    return (
      <Card>
        <CardContent className="p-8 text-center">
          <CheckCircle2 className="h-12 w-12 mx-auto text-emerald-500 mb-3" />
          <h3 className="text-lg font-semibold mb-1">No Wrong Answers!</h3>
          <p className="text-sm text-muted-foreground">
            Great job! You haven&apos;t answered any questions incorrectly yet.
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between mb-2">
        <p className="text-sm text-muted-foreground">
          {wrongQuestions.length} wrong answer{wrongQuestions.length !== 1 ? 's' : ''}
        </p>
      </div>

      <ScrollArea className="max-h-[calc(100vh-220px)]">
        <Accordion
          type="multiple"
          value={expandedItems}
          onValueChange={setExpandedItems}
          className="space-y-3"
        >
          {wrongQuestions.map((q, index) => (
            <motion.div
              key={q.hq_id || index}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.03 }}
            >
              <Card className="overflow-hidden">
                <AccordionItem value={q.hq_id || String(index)} className="border-none">
                  <AccordionTrigger className="px-4 py-3 hover:no-underline">
                    <div className="flex-1 text-left">
                      <div className="flex items-start gap-2">
                        <XCircle className="h-4 w-4 text-red-500 mt-0.5 shrink-0" />
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium line-clamp-2">
                            {q.question}
                          </p>
                          <div className="flex items-center gap-2 mt-2 flex-wrap">
                            <Badge variant="destructive" className="text-xs">
                              You: {q.yourAnswer}
                            </Badge>
                            <Badge className="bg-emerald-100 text-emerald-700 dark:bg-emerald-900 dark:text-emerald-300 text-xs border-0">
                              Correct: {q.correctAnswer}
                            </Badge>
                          </div>
                        </div>
                      </div>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="px-4 pb-4 space-y-3">
                      {/* Options list */}
                      {q.options && q.options.length > 0 && (
                        <div className="space-y-1.5">
                          <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">All Options</p>
                          {q.options.map((opt, optIdx) => (
                            <div
                              key={optIdx}
                              className={`text-sm px-3 py-2 rounded-lg ${
                                optIdx === q.correct_option
                                  ? 'bg-emerald-50 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800'
                                  : optIdx === q.selected_option
                                    ? 'bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-300 border border-red-200 dark:border-red-800'
                                    : 'bg-muted/50 text-muted-foreground'
                              }`}
                            >
                              <span className="font-medium">{String.fromCharCode(65 + optIdx)}.</span> {opt}
                              {optIdx === q.correct_option && (
                                <CheckCircle2 className="inline h-3.5 w-3.5 ml-1.5 text-emerald-500" />
                              )}
                              {optIdx === q.selected_option && optIdx !== q.correct_option && (
                                <XCircle className="inline h-3.5 w-3.5 ml-1.5 text-red-500" />
                              )}
                            </div>
                          ))}
                        </div>
                      )}

                      {/* Explanation */}
                      {q.explanation && (
                        <div className="space-y-2">
                          <div className="flex items-center gap-1.5">
                            <Lightbulb className="h-4 w-4 text-yellow-500" />
                            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
                              Explanation
                            </p>
                          </div>
                          {q.explanation.text && (
                            <p className="text-sm text-foreground/90 leading-relaxed">
                              {q.explanation.text}
                            </p>
                          )}
                          {/* Photos */}
                          {q.explanation.photos && q.explanation.photos.length > 0 && (
                            <div className="grid grid-cols-2 gap-2">
                              {q.explanation.photos.map((photo, pi) => (
                                <img
                                  key={pi}
                                  src={photo}
                                  alt={`Explanation photo ${pi + 1}`}
                                  className="rounded-lg max-h-40 w-full object-cover"
                                />
                              ))}
                            </div>
                          )}
                          {/* Images */}
                          {q.explanation.images && q.explanation.images.length > 0 && (
                            <div className="grid grid-cols-2 gap-2">
                              {q.explanation.images.map((img, ii) => (
                                <img
                                  key={ii}
                                  src={img}
                                  alt={`Explanation image ${ii + 1}`}
                                  className="rounded-lg max-h-40 w-full object-cover"
                                />
                              ))}
                            </div>
                          )}
                          {/* Videos */}
                          {q.explanation.videos && q.explanation.videos.length > 0 && (
                            <div className="space-y-2">
                              {q.explanation.videos.map((vid, vi) => (
                                <a
                                  key={vi}
                                  href={vid}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="flex items-center gap-2 text-sm text-blue-500 hover:underline"
                                >
                                  <ExternalLink className="h-3.5 w-3.5" />
                                  Video {vi + 1}
                                </a>
                              ))}
                            </div>
                          )}
                        </div>
                      )}

                      {/* Retry Button Placeholder */}
                      <Button
                        variant="outline"
                        size="sm"
                        className="w-full mt-2"
                        disabled
                      >
                        <RotateCcw className="h-3.5 w-3.5 mr-1.5" />
                        Retry (Coming Soon)
                      </Button>
                    </div>
                  </AccordionContent>
                </AccordionItem>
              </Card>
            </motion.div>
          ))}
        </Accordion>
      </ScrollArea>
    </div>
  );
}
