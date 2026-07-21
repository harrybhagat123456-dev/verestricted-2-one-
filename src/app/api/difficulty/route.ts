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

    const difficulties = await db.collection('dashboard_user_answers').aggregate([
      { $match: { user_id: numUserId } },
      { $lookup: { from: 'dashboard_polls', localField: 'hq_id', foreignField: 'hq_id', as: 'poll' } },
      { $unwind: { path: '$poll', preserveNullAndEmptyArrays: true } },
      { $group: {
        _id: '$poll.difficulty',
        total: { $sum: 1 },
        correct: { $sum: { $cond: ['$is_correct', 1, 0] } },
        avgResponseTime: { $avg: '$response_time_seconds' },
      }},
      { $addFields: {
        accuracy: { $multiply: [{ $divide: ['$correct', '$total'] }, 100] },
      }},
      { $sort: { _id: 1 } },
    ]).toArray();

    const formatted = difficulties.map((d) => ({
      difficulty: d._id || 'Unrated',
      total: d.total,
      correct: d.correct,
      accuracy: Math.round(d.accuracy * 100) / 100,
      avgResponseTime: d.avgResponseTime ? Math.round(d.avgResponseTime * 100) / 100 : 0,
    }));

    return NextResponse.json({ difficulties: formatted });
  } catch (error) {
    console.error('Difficulty error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
