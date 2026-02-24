import { create } from "zustand";
import { v4 as uuid } from "uuid";

export type Message = {
  id: string; // client id or server history item id
  sender: "user" | "bot";
  text: string;

  // backend related
  serverId?: number;
  promptText?: string;

  // ui
  liked?: "like" | "dislike";
  error?: boolean;
  timestamp?: number; // epoch ms
};

type ChatStore = {
  messages: Message[];
  loading: boolean;

  // ✅ async now (DB se load karega)
  loadFromStorage: () => Promise<void>;

  sendMessage: (text: string) => Promise<void>;
  clearChat: () => void;
  reactToMessage: (id: string, reaction: Message["liked"]) => void;
  sendFeedback: (
    id: string,
    feedback: "like" | "dislike",
    additional_feedback?: string
  ) => Promise<void>;
};

const DEFAULT_BOT_MESSAGE: Message = {
  id: "welcome-message",
  sender: "bot",
  text: "Hey! 👋 I can help you analyze Amazon sales, fees, taxes, profit, and trends. What would you like to explore?",
  timestamp: Date.now(),
};

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:5000";

const getAuthToken = () =>
  typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

// ✅ backend history fetcher (uses your existing API)
const fetchHistoryFromDB = async (): Promise<Message[] | null> => {
  const token = getAuthToken();
  if (!token) return null;

  try {
    const res = await fetch(
      `${API_BASE_URL.replace(/\/$/, "")}/chatbot/history?limit=500&offset=0`,
      {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` },
      }
    );

    const data = await res.json().catch(() => null);
    if (!res.ok || !data?.success || !Array.isArray(data?.items)) return null;

    const items: Message[] = data.items.map((m: any) => ({
      id: String(m.id), // backend returns "12-u" / "12-b"
      sender: m.sender,
      text: m.text,
      timestamp: m.timestamp ? Date.parse(m.timestamp) : Date.now(),
    }));

    return items.length ? items : null;
  } catch {
    return null;
  }
};

export const useChatbotStore = create<ChatStore>((set, get) => ({
  messages: [],
  loading: false,

  // ✅ DB -> local fallback -> default
  loadFromStorage: async () => {
    // agar pehle se messages loaded hain → kuch mat karo
    if (get().messages.length > 0) return;

    // 1) Try DB history first
    const dbMsgs = await fetchHistoryFromDB();
    if (dbMsgs && dbMsgs.length > 0) {
      set({ messages: dbMsgs });
      if (typeof window !== "undefined") {
        localStorage.setItem("chatbot_history", JSON.stringify(dbMsgs));
      }
      return;
    }

    // 2) fallback localStorage
    const saved =
      typeof window !== "undefined"
        ? localStorage.getItem("chatbot_history")
        : null;

    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length > 0) {
          set({ messages: parsed });
          return;
        }
      } catch {
        // ignore
      }
    }

    // 3) default welcome
    set({ messages: [DEFAULT_BOT_MESSAGE] });
  },

  sendMessage: async (text) => {
    if (!text.trim()) return;

    const userMsg: Message = {
      id: uuid(),
      sender: "user",
      text,
      timestamp: Date.now(),
    };

    // ✅ add user msg + sync local
    set((s) => {
      const updated = [...s.messages, userMsg];
      if (typeof window !== "undefined") {
        localStorage.setItem("chatbot_history", JSON.stringify(updated));
      }
      return { messages: updated, loading: true };
    });

    try {
      const res = await fetch(`${API_BASE_URL}/chatbot`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getAuthToken()}`,
        },
        body: JSON.stringify({
          action: "chat",
          query: text,
        }),
      });

      const data = await res.json();

      const botMsg: Message = {
        id: uuid(),
        sender: "bot",
        text: data?.response || "No response",
        timestamp: Date.now(),
        // (optional) agar backend return karta ho
        serverId: data?.message_id,
        promptText: data?.original_prompt,
      };

      // ✅ add bot msg + sync local (no stale get().messages)
      set((s) => {
        const updated = [...s.messages, botMsg];
        if (typeof window !== "undefined") {
          localStorage.setItem("chatbot_history", JSON.stringify(updated));
        }
        return { messages: updated, loading: false };
      });
    } catch {
      set({ loading: false });
    }
  },

  clearChat: () => {
    if (typeof window !== "undefined") {
      localStorage.removeItem("chatbot_history");
    }
    // ✅ keep welcome instead of empty (optional but better UX)
    set({ messages: [DEFAULT_BOT_MESSAGE] });
  },

  reactToMessage: (id, reaction) => {
    set((s) => {
      const updated = s.messages.map((m) =>
        m.id === id ? { ...m, liked: reaction } : m
      );
      if (typeof window !== "undefined") {
        localStorage.setItem("chatbot_history", JSON.stringify(updated));
      }
      return { messages: updated };
    });
  },

  sendFeedback: async (id, feedback, additional_feedback) => {
    const msg = get().messages.find((m) => m.id === id);
    if (!msg?.serverId) return;

    try {
      await fetch(`${API_BASE_URL}/chatbot/feedback`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${getAuthToken()}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          message_id: msg.serverId,
          feedback,
          original_prompt: msg.promptText,
          additional_feedback,
        }),
      });
    } catch {
      // ignore
    }
  },
}));
