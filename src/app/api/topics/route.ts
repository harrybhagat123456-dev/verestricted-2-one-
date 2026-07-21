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

    const topics = await db.collection('dashboard_user_answers').aggregate([
      { $match: { user_id: numUserId } },
      { $lookup: { from: 'dashboard_polls', localField: 'hq_id', foreignField: 'hq_id', as: 'poll' } },
      { $unwind: { path: '$poll', preserveNullAndEmptyArrays: true } },
      { $group: {
        _id: '$poll.topic',
        total: { $sum: 1 },
        correct: { $sum: { $cond: ['$is_correct', 1, 0] } },
      }},
      { $addFields: {
        accuracy: { $multiply: [{ $divide: ['$correct', '$total'] }, 100] },
      }},
      { $sort: { total: -1 } },
    ]).toArray();

    const formatted = topics.map((t) => ({
      topic: t._id || 'Uncategorized',
      total: t.total,
      correct: t.correct,
      accuracy: Math.round(t.accuracy * 100) / 100,
    }));

    return NextResponse.json({ topics: formatted });
  } catch (error) {
    console.error('Topics error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
