import { NextRequest, NextResponse } from 'next/server';
import { getDb } from '@/lib/mongodb';

export async function GET(request: NextRequest) {
  try {
    const userId = request.nextUrl.searchParams.get('user_id');
    if (!userId) {
      return NextResponse.json({ error: 'Missing user_id parameter' }, { status: 400 });
    }

    const db = await getDb();
    const numUserId = Number(userId);

    const wrongAnswers = await db.collection('dashboard_user_answers').aggregate([
      { $match: { user_id: numUserId, is_correct: false } },
      { $lookup: { from: 'dashboard_polls', localField: 'hq_id', foreignField: 'hq_id', as: 'poll' } },
      { $unwind: { path: '$poll', preserveNullAndEmptyArrays: true } },
      { $lookup: { from: 'dashboard_question_explanations', localField: 'hq_id', foreignField: 'hq_id', as: 'explanation' } },
      { $unwind: { path: '$explanation', preserveNullAndEmptyArrays: true } },
      { $sort: { answered_at: -1 } },
      { $limit: 100 },
    ]).toArray();

    const formatted = wrongAnswers.map((doc) => ({
      hq_id: doc.hq_id,
      question: doc.poll?.question || 'Unknown question',
      options: doc.poll?.options || [],
      yourAnswer: doc.selected_option !== undefined && doc.poll?.options
        ? doc.poll.options[doc.selected_option] || `Option ${doc.selected_option}`
        : 'N/A',
      correctAnswer: doc.correct_option !== undefined && doc.poll?.options
        ? doc.poll.options[doc.correct_option] || `Option ${doc.correct_option}`
        : 'N/A',
      selected_option: doc.selected_option,
      correct_option: doc.correct_option,
      explanation: doc.explanation
        ? {
            text: doc.explanation.text || '',
            images: doc.explanation.images || [],
            videos: doc.explanation.videos || [],
            photos: doc.explanation.photos || [],
            kind: doc.explanation.kind || '',
          }
        : null,
      answered_at: doc.answered_at,
    }));

    return NextResponse.json({ wrongQuestions: formatted });
  } catch (error) {
    console.error('Wrong questions error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
