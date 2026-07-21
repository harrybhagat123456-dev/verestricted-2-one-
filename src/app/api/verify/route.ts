import { NextRequest, NextResponse } from 'next/server';
import { getDb } from '@/lib/mongodb';

export async function GET(request: NextRequest) {
  try {
    const secret = request.nextUrl.searchParams.get('secret');
    if (!secret) {
      return NextResponse.json({ error: 'Missing secret parameter' }, { status: 400 });
    }

    const db = await getDb();
    const user = await db.collection('dashboard_users').findOne(
      { dashboard_secret: secret },
      { projection: { user_id: 1, username: 1, first_name: 1 } }
    );

    if (!user) {
      return NextResponse.json({ error: 'Invalid dashboard link' }, { status: 404 });
    }

    return NextResponse.json({
      user_id: user.user_id,
      username: user.username,
      first_name: user.first_name,
    });
  } catch (error) {
    console.error('Verify error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
