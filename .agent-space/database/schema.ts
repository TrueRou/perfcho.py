import { pgTable, index, unique, integer, char, varchar, numeric, smallint, boolean, timestamp, bigint, uniqueIndex, bigserial, foreignKey, json, text, primaryKey, pgSequence, pgEnum } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"

export const channelHandleType = pgEnum("channel_handle_type", ['join', 'send_message', 'kick_user', 'mute_user'])
export const channelType = pgEnum("channel_type", ['private', 'public', 'group', 'multiplayer', 'spectaor'])
export const gameMode = pgEnum("game_mode", ['Standard', 'Taiko', 'Fruits', 'Mania'])
export const ppVersion = pgEnum("pp_version", ['v1', 'v2'])
export const rankStatus = pgEnum("rank_status", ['Graveyard', 'Wip', 'Pending', 'Ranked', 'Approved', 'Qualified', 'Loved'])
export const rankingType = pgEnum("ranking_type", ['score_v1', 'score_v2', 'pp_v1', 'pp_v2'])
export const scoreGrade = pgEnum("score_grade", ['A', 'B', 'C', 'D', 'S', 'SH', 'X', 'XH', 'F'])
export const scoreVersion = pgEnum("score_version", ['v1', 'v2'])

export const usersIdSeq1 = pgSequence("users_id_seq1", {  startWith: "1", increment: "1", minValue: "1", maxValue: "2147483647", cache: "1", cycle: false })
export const scoresIdSeq1 = pgSequence("scores_id_seq1", {  startWith: "1", increment: "1", minValue: "1", maxValue: "9223372036854775807", cache: "1", cycle: false })

export const beatmaps = pgTable("beatmaps", {
	id: integer().primaryKey().notNull(),
	sid: integer().notNull(),
	md5: char({ length: 32 }).notNull(),
	title: varchar(),
	fileName: varchar("file_name").notNull(),
	artist: varchar(),
	diffName: varchar("diff_name").notNull(),
	originServer: varchar("origin_server").notNull(),
	mapperName: varchar("mapper_name").notNull(),
	mapperId: varchar("mapper_id").notNull(),
	rankStatus: rankStatus("rank_status").default('Pending').notNull(),
	gameMode: gameMode("game_mode").notNull(),
	stars: numeric({ precision: 16, scale:  2 }).notNull(),
	bpm: numeric({ precision: 16, scale:  2 }).notNull(),
	cs: numeric({ precision: 4, scale:  2 }).notNull(),
	od: numeric({ precision: 4, scale:  2 }).notNull(),
	ar: numeric({ precision: 4, scale:  2 }).notNull(),
	hp: numeric({ precision: 4, scale:  2 }).notNull(),
	length: integer().notNull(),
	lengthDrain: integer("length_drain").notNull(),
	source: varchar(),
	tags: varchar(),
	genreId: smallint("genre_id"),
	languageId: smallint("language_id"),
	storyboard: boolean(),
	video: boolean(),
	objectCount: integer("object_count"),
	sliderCount: integer("slider_count"),
	spinnerCount: integer("spinner_count"),
	maxCombo: integer("max_combo"),
	immutable: boolean().default(false).notNull(),
	lastUpdate: timestamp("last_update", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	uploadTime: timestamp("upload_time", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	approvedTime: timestamp("approved_time", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	titleUnicode: varchar("title_unicode"),
	artistUnicode: varchar("artist_unicode"),
}, (table) => [
	index("IDX_beatmaps_file_name").using("btree", table.fileName.asc().nullsLast().op("text_ops")),
	index("IDX_beatmaps_rank_status").using("btree", table.rankStatus.asc().nullsLast().op("enum_ops")),
	index("IDX_beatmaps_sid").using("btree", table.sid.asc().nullsLast().op("int4_ops")),
	unique("beatmaps_md5_key").on(table.md5),
]);

export const channels = pgTable("channels", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().notNull(),
	channelType: channelType("channel_type").notNull(),
	name: varchar(),
	description: varchar(),
	icon: varchar(),
	autoJoin: boolean("auto_join").default(false).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	creatorId: bigint("creator_id", { mode: "number" }),
}, (table) => [
	index("IDX_channel_name").using("btree", table.name.asc().nullsLast().op("text_ops")),
	unique("channels_name_key").on(table.name),
]);

export const privileges = pgTable("privileges", {
	id: bigserial({ mode: "bigint" }).primaryKey().notNull(),
	name: varchar().notNull(),
	description: varchar(),
	priority: smallint().default(1000).notNull(),
	creatorId: integer("creator_id"),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	uniqueIndex("IDX_privileges_name").using("btree", table.name.asc().nullsLast().op("text_ops")),
	unique("privileges_name_key").on(table.name),
]);

export const scores = pgTable("scores", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedAlwaysAsIdentity({ name: "scores_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	mapHash: char("map_hash", { length: 32 }).notNull(),
	userId: integer("user_id").notNull(),
	cksm: varchar().notNull(),
	kind: varchar().notNull(),
	playTime: integer("play_time").notNull(),
	completed: boolean().default(false).notNull(),
	verifiedAt: timestamp("verified_at", { precision: 6, withTimezone: true, mode: 'string' }),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	uniqueIndex("IDX_scores_cksm").using("btree", table.cksm.asc().nullsLast().op("text_ops")),
	index("IDX_scores_user_id").using("btree", table.userId.asc().nullsLast().op("int4_ops")),
	index("idx_scores_user_map_hash").using("btree", table.userId.asc().nullsLast().op("int4_ops"), table.mapHash.asc().nullsLast().op("bpchar_ops")),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_scores_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	unique("scores_cksm_key").on(table.cksm),
]);

export const scoresClassic = pgTable("scores_classic", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().notNull(),
	mode: gameMode().notNull(),
	scoreVersion: scoreVersion("score_version").notNull(),
	score: integer().notNull(),
	accuracy: numeric({ precision: 6, scale:  2 }).notNull(),
	combo: integer().notNull(),
	mods: integer().notNull(),
	n300: integer().notNull(),
	n100: integer().notNull(),
	n50: integer().notNull(),
	miss: integer().notNull(),
	geki: integer().notNull(),
	katu: integer().notNull(),
	perfect: boolean().default(false).notNull(),
	grade: scoreGrade().notNull(),
	clientFlags: integer("client_flags").notNull(),
	clientVersion: varchar("client_version").notNull(),
}, (table) => [
	index("idx_scores_classic_autopilot").using("btree", table.id.asc().nullsLast().op("int8_ops")).where(sql`((mods & 8192) <> 0)`),
	index("idx_scores_classic_id_mode_mods").using("btree", table.id.asc().nullsLast().op("int4_ops"), table.mode.asc().nullsLast().op("int4_ops"), table.mods.asc().nullsLast().op("int4_ops")),
	index("idx_scores_classic_normal").using("btree", table.id.asc().nullsLast().op("int8_ops")).where(sql`((mods & 8320) = 0)`),
	index("idx_scores_classic_relax").using("btree", table.id.asc().nullsLast().op("int8_ops")).where(sql`((mods & 128) <> 0)`),
	foreignKey({
			columns: [table.id],
			foreignColumns: [scores.id],
			name: "FK_scores_classic_scores_id"
		}).onUpdate("cascade").onDelete("cascade"),
]);

export const scoresGeneric = pgTable("scores_generic", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().notNull(),
	mode: varchar().notNull(),
	json: json().notNull(),
}, (table) => [
	foreignKey({
			columns: [table.id],
			foreignColumns: [scores.id],
			name: "FK_scores_generic_scores_id"
		}).onUpdate("cascade").onDelete("cascade"),
]);

export const chatMessages = pgTable("chat_messages", {
	id: bigserial({ mode: "bigint" }).primaryKey().notNull(),
	senderId: integer("sender_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	channelId: bigint("channel_id", { mode: "number" }).notNull(),
	timestamp: timestamp({ precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	contentString: text("content_string").notNull(),
	contentHtml: text("content_html"),
	isAction: boolean("is_action").default(false).notNull(),
}, (table) => [
	uniqueIndex("IDX_chat_msg_channel_id").using("btree", table.channelId.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.channelId],
			foreignColumns: [channels.id],
			name: "FK_chat_msg_channel_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.senderId],
			foreignColumns: [users.id],
			name: "FK_chat_msg_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
]);

export const userPrivileges = pgTable("user_privileges", {
	userId: integer("user_id").primaryKey().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	privilegeId: bigint("privilege_id", { mode: "number" }).notNull(),
	grantorId: integer("grantor_id").notNull(),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	index("IDX_user_priv_priv_id").using("btree", table.privilegeId.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.grantorId],
			foreignColumns: [users.id],
			name: "FK_user_priv_grantor_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.privilegeId],
			foreignColumns: [privileges.id],
			name: "FK_user_priv_priv_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_user_priv_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
]);

export const userSettings = pgTable("user_settings", {
	userId: integer("user_id").primaryKey().notNull(),
	displayUnicodeName: boolean("display_unicode_name").default(false).notNull(),
	scoreboardRankingType: rankingType("scoreboard_ranking_type").default('score_v1').notNull(),
	invisibleOnline: boolean("invisible_online").default(false).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_user_settings_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
]);

export const users = pgTable("users", {
	id: integer().primaryKey().generatedAlwaysAsIdentity({ name: "users_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 2147483647, cache: 1 }),
	name: varchar({ length: 16 }).notNull(),
	nameSafe: varchar("name_safe", { length: 16 }).notNull(),
	nameUnicode: varchar("name_unicode", { length: 10 }),
	nameUnicodeSafe: varchar("name_unicode_safe", { length: 10 }),
	password: varchar().notNull(),
	email: varchar({ length: 64 }).notNull(),
	country: varchar({ length: 8 }),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	index("IDX_user_pp_user_id").using("btree", table.id.asc().nullsLast().op("int4_ops")),
	uniqueIndex("IDX_users_email").using("btree", table.email.asc().nullsLast().op("text_ops")),
	uniqueIndex("IDX_users_name_safe").using("btree", table.nameSafe.asc().nullsLast().op("text_ops")),
	uniqueIndex("IDX_users_name_unicode_safe").using("btree", table.nameUnicodeSafe.asc().nullsLast().op("text_ops")),
	unique("users_name_key").on(table.name),
	unique("users_name_safe_key").on(table.nameSafe),
	unique("users_name_unicode_key").on(table.nameUnicode),
	unique("users_name_unicode_safe_key").on(table.nameUnicodeSafe),
	unique("users_email_key").on(table.email),
]);

export const beatmapsets = pgTable("beatmapsets", {
	id: integer().notNull(),
	title: varchar(),
	artist: varchar(),
	originServer: varchar("origin_server").notNull(),
	mapperName: varchar("mapper_name").notNull(),
	mapperId: varchar("mapper_id").notNull(),
	source: varchar(),
	genreId: smallint("genre_id"),
	languageId: smallint("language_id"),
	artistUnicode: varchar("artist_unicode"),
	titleUnicode: varchar("title_unicode"),
});

export const serverSettings = pgTable("server_settings", {
	key: varchar().primaryKey().notNull(),
	value: varchar().notNull(),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
});

export const channelUsers = pgTable("channel_users", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	channelId: bigint("channel_id", { mode: "number" }).notNull(),
	userId: integer("user_id").notNull(),
}, (table) => [
	index("IDX_channel_users_user_id").using("btree", table.userId.asc().nullsLast().op("int4_ops")),
	foreignKey({
			columns: [table.channelId],
			foreignColumns: [channels.id],
			name: "FK_channel_users_channel_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_channel_users_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.channelId], name: "channel_users_pkey"}),
]);

export const channelPrivileges = pgTable("channel_privileges", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	channelId: bigint("channel_id", { mode: "number" }).notNull(),
	handle: channelHandleType().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	requiredPrivilegeId: bigint("required_privilege_id", { mode: "number" }).notNull(),
}, (table) => [
	index("IDX_channel_priv_priv_id").using("btree", table.requiredPrivilegeId.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.channelId],
			foreignColumns: [channels.id],
			name: "FK_channel_priv_channel_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.requiredPrivilegeId],
			foreignColumns: [privileges.id],
			name: "FK_channel_priv_priv_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.handle, table.channelId], name: "channel_privileges_pkey"}),
]);

export const favouriteBeatmaps = pgTable("favourite_beatmaps", {
	userId: integer("user_id").notNull(),
	beatmapsetId: integer("beatmapset_id").notNull(),
	comment: varchar({ length: 15 }),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	index("IDX_favourite_beatmaps_user_id").using("btree", table.userId.asc().nullsLast().op("int4_ops")),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_favourite_beatmaps_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.beatmapsetId], name: "favourite_beatmaps_pkey"}),
]);

export const followers = pgTable("followers", {
	userId: integer("user_id").notNull(),
	followId: integer("follow_id").notNull(),
	remark: varchar({ length: 16 }),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	index("IDX_followers_user_id").using("btree", table.userId.asc().nullsLast().op("int4_ops")),
	index("idx_followers_user_follow_id").using("btree", table.userId.asc().nullsLast().op("int4_ops"), table.followId.asc().nullsLast().op("int4_ops")),
	foreignKey({
			columns: [table.followId],
			foreignColumns: [users.id],
			name: "FK_followers_follow_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_followers_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.followId], name: "followers_pkey"}),
]);

export const beatmapRatings = pgTable("beatmap_ratings", {
	userId: integer("user_id").notNull(),
	mapMd5: char("map_md5", { length: 32 }).notNull(),
	rating: smallint().notNull(),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	index("IDX_beatmap_ratings_map_md5").using("btree", table.mapMd5.asc().nullsLast().op("bpchar_ops")),
	foreignKey({
			columns: [table.mapMd5],
			foreignColumns: [beatmaps.md5],
			name: "FK_beatmap_ratings_map_md5"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_beatmap_ratings_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.mapMd5], name: "beatmap_ratings_pkey"}),
]);

export const scorePp = pgTable("score_pp", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	scoreId: bigint("score_id", { mode: "number" }).notNull(),
	mode: varchar().notNull(),
	ppVersion: varchar("pp_version").notNull(),
	pp: numeric({ precision: 16, scale:  2 }).notNull(),
	rawPp: json("raw_pp"),
}, (table) => [
	index("IDX_score_pp_score_id").using("btree", table.scoreId.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.scoreId],
			foreignColumns: [scores.id],
			name: "FK_score_pp_scores_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.scoreId, table.ppVersion, table.mode], name: "score_pp_pkey"}),
]);

export const leaderboard = pgTable("leaderboard", {
	beatmapId: integer("beatmap_id").notNull(),
	mode: varchar().notNull(),
	rankingType: varchar("ranking_type").notNull(),
	userId: integer("user_id").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	scoreId: bigint("score_id", { mode: "number" }).notNull(),
}, (table) => [
	index("IDX_leaderboard_beatmap_id").using("btree", table.beatmapId.asc().nullsLast().op("int4_ops")),
	index("IDX_leaderboard_mode_ranking").using("btree", table.mode.asc().nullsLast().op("text_ops"), table.rankingType.asc().nullsLast().op("text_ops")),
	index("IDX_leaderboard_score_id").using("btree", table.scoreId.asc().nullsLast().op("int8_ops")),
	index("IDX_leaderboard_user_id").using("btree", table.userId.asc().nullsLast().op("int4_ops")),
	foreignKey({
			columns: [table.beatmapId],
			foreignColumns: [beatmaps.id],
			name: "FK_leaderboard_beatmap_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.scoreId],
			foreignColumns: [scores.id],
			name: "FK_leaderboard_score_id"
		}).onUpdate("cascade").onDelete("cascade"),
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_leaderboard_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.rankingType, table.mode, table.beatmapId], name: "leaderboard_pkey"}),
]);

export const userPp = pgTable("user_pp", {
	userId: integer("user_id").notNull(),
	mode: varchar().notNull(),
	ppVersion: varchar("pp_version").notNull(),
	pp: numeric({ precision: 16, scale:  2 }).notNull(),
	rawPp: json("raw_pp"),
}, (table) => [
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_user_pp_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.ppVersion, table.mode], name: "user_pp_pkey"}),
]);

export const banchoClientHardwareRecords = pgTable("bancho_client_hardware_records", {
	userId: integer("user_id").notNull(),
	timeOffset: integer("time_offset").notNull(),
	pathHash: char("path_hash", { length: 32 }).notNull(),
	adapters: varchar().notNull(),
	adaptersHash: char("adapters_hash", { length: 32 }).notNull(),
	uninstallId: char("uninstall_id", { length: 32 }).notNull(),
	diskId: char("disk_id", { length: 32 }).notNull(),
	usedTimes: integer("used_times").default(1).notNull(),
	createdAt: timestamp("created_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_bancho_client_hardware_records_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.uninstallId, table.pathHash, table.diskId, table.adaptersHash], name: "bancho_client_hardware_records_pkey"}),
]);

/**
 * User stats for classic game modes
 *
 * Stores statistics for traditional osu! game modes with standard scoring fields
 * (n300, n100, n50, miss, geki, katu, etc.).
 *
 * Primary key: (userId, scoreboard)
 * - userId: Reference to users table
 * - scoreboard: Represents the ruleset | mods combination (e.g., 'osu!', 'osu!rx', 'taiko', 'mania4k')
 *
 * Scoreboard values (from database-design.md):
 * - 'osu!', 'osu!rx', 'osu!ap': osu! standard/relax/autopilot
 * - 'taiko', 'taikorx': Taiko standard/relax
 * - 'fruits', 'fruitsrx': Fruits standard/relax
 * - 'mania', 'mania4k', 'mania5k': Mania with different key counts
 *
 * TODO: Confirm exact scoreboard values for mania key modes
 * TODO: Verify values match database-design.md mapping once finalized
 */
export const userStatsClassic = pgTable("user_stats_classic", {
	userId: integer("user_id").notNull(),
	scoreboard: varchar("scoreboard").notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	totalScore: bigint("total_score", { mode: "number" }).notNull().default(0),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	rankedScore: bigint("ranked_score", { mode: "number" }).notNull().default(0),
	playcount: integer().notNull().default(0),
	totalHits: integer("total_hits").notNull().default(0),
	accuracy: numeric({ precision: 6, scale:  2 }).notNull().default('0'),
	maxCombo: integer("max_combo").notNull().default(0),
	count300: integer().notNull().default(0),
	count100: integer().notNull().default(0),
	count50: integer().notNull().default(0),
	countMiss: integer("count_miss").notNull().default(0),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_user_stats_classic_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.scoreboard], name: "user_stats_classic_pkey"}),
	index("idx_user_stats_classic_scoreboard").on(table.scoreboard),
]);

/**
 * User stats for generic game modes (placeholder for future use)
 *
 * This table is reserved for future game modes that don't fit the classic scoring model.
 * Statistics will be stored as JSON for flexibility.
 *
 * Examples of future use:
 * - Non-traditional game modes
 * - Custom scoring systems
 * - Experimental modes
 *
 * TODO: Implement when generic game modes are needed
 */
export const userStatsGeneric = pgTable("user_stats_generic", {
	userId: integer("user_id").notNull(),
	scoreboard: varchar("scoreboard").notNull(),
	json: json().notNull(),
	updatedAt: timestamp("updated_at", { precision: 6, withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`).notNull(),
}, (table) => [
	foreignKey({
			columns: [table.userId],
			foreignColumns: [users.id],
			name: "FK_user_stats_generic_user_id"
		}).onUpdate("cascade").onDelete("cascade"),
	primaryKey({ columns: [table.userId, table.scoreboard], name: "user_stats_generic_pkey"}),
]);
