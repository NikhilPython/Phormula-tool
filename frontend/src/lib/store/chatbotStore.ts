import { create } from "zustand";
import { v4 as uuid } from "uuid";

export type Message = {
  id: string;
  sender: "user" | "bot";
  text: string;

  serverId?: number;
  promptText?: string;

  liked?: "like" | "dislike";
  error?: boolean;
  timestamp?: number;
  suggestedQuestions?: string[];
};

export type SendMessageContext = {
  country?: string | null;
};

type ChatStore = {
  messages: Message[];
  loading: boolean;

  loadFromStorage: () => Promise<void>;
  sendMessage: (text: string, context?: SendMessageContext) => Promise<void>;
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

const normalizeCountry = (value: unknown): string | null => {
  const raw = String(value ?? "").trim();
  if (!raw || raw.toLowerCase() === "undefined" || raw.toLowerCase() === "null") {
    return null;
  }

  let decoded = raw;
  try {
    decoded = decodeURIComponent(raw);
  } catch {}

  const cleaned = decoded
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");

  const aliases: Record<string, string> = {
    uk: "uk",
    gb: "uk",
    gbr: "uk",
    "amazon uk": "uk",
    "united kingdom": "uk",
    us: "us",
    usa: "us",
    "amazon us": "us",
    "united states": "us",
    "united states of america": "us",
    global: "global",
    all: "global",
    "all countries": "global",
  };

  return aliases[cleaned] || cleaned.replace(/\s+/g, "");
};

const getCountryFromCurrentPath = (): string | null => {
  if (typeof window === "undefined") return null;

  const segments = window.location.pathname
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean);
  const chatbotIndex = segments.findIndex(
    (segment) => segment.toLowerCase() === "chatbot"
  );

  if (chatbotIndex >= 0) {
    return normalizeCountry(segments[chatbotIndex + 2]);
  }

  return null;
};

const getCountryFromStorage = (): string | null => {
  if (typeof window === "undefined") return null;

  for (const key of ["countryName", "selectedCountryCode", "selectedCountry", "country"]) {
    const country = normalizeCountry(localStorage.getItem(key));
    if (country) return country;
  }

  try {
    const userData = JSON.parse(localStorage.getItem("userdata") || "{}");
    return normalizeCountry(userData?.country);
  } catch {
    return null;
  }
};

const resolveActiveCountry = (contextCountry?: string | null): string => {
  return (
    normalizeCountry(contextCountry) ||
    getCountryFromCurrentPath() ||
    getCountryFromStorage() ||
    "uk"
  );
};

// ✅ Fetch history (unchanged)
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
      id: String(m.id),
      sender: m.sender,
      text: m.text,
      serverId:
        m.sender === "bot"
          ? Number.parseInt(String(m.id).split("-")[0], 10) || undefined
          : undefined,
      timestamp: m.timestamp ? Date.parse(m.timestamp) : Date.now(),
      suggestedQuestions: Array.isArray(m.suggested_questions)
        ? m.suggested_questions.filter((q: unknown) => typeof q === "string")
        : undefined,
    }));

    return items.length ? items : null;
  } catch {
    return null;
  }
};

export const useChatbotStore = create<ChatStore>((set, get) => ({
  messages: [],
  loading: false,

  loadFromStorage: async () => {
    if (get().messages.length > 0) return;

    const dbMsgs = await fetchHistoryFromDB();
    if (dbMsgs && dbMsgs.length > 0) {
      set({ messages: dbMsgs });
      if (typeof window !== "undefined") {
        localStorage.setItem("chatbot_history", JSON.stringify(dbMsgs));
      }
      return;
    }

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
      } catch {}
    }

    set({ messages: [DEFAULT_BOT_MESSAGE] });
  },

  // 🚀 UPDATED SEND MESSAGE (AGENT)
  sendMessage: async (text, context) => {
    if (!text.trim()) return;

    const userMsg: Message = {
      id: uuid(),
      sender: "user",
      text,
      timestamp: Date.now(),
    };

    // add user msg
    set((s) => {
      const updated = [...s.messages, userMsg];
      if (typeof window !== "undefined") {
        localStorage.setItem("chatbot_history", JSON.stringify(updated));
      }
      return { messages: updated, loading: true };
    });

    try {
      // 👉 get user country dynamically
      const country = resolveActiveCountry(context?.country);
      const res = await fetch(`${API_BASE_URL}/api/agent/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getAuthToken()}`,
        },
        body: JSON.stringify({
          message: text,
          country,
          conversation_id: null, // 🔥 can upgrade later
          email_requested: false,
          thresholds: {},
        }),
      });

      const data = await res.json();
      const suggestedQuestions = Array.isArray(data?.suggested_questions)
        ? data.suggested_questions.filter((q: unknown) => typeof q === "string")
        : [];

      const botMsg: Message = {
        id: uuid(),
        sender: "bot",
        text:
          data?.output ||
          data?.response ||
          data?.result ||
          data?.message ||
          "No response",
        serverId: typeof data?.history_id === "number" ? data.history_id : undefined,
        promptText: text,
        suggestedQuestions,
        timestamp: Date.now(),
      };

      set((s) => {
        const updated = [...s.messages, botMsg];
        if (typeof window !== "undefined") {
          localStorage.setItem("chatbot_history", JSON.stringify(updated));
        }
        return { messages: updated, loading: false };
      });
    } catch (err) {
      const errorMsg: Message = {
        id: uuid(),
        sender: "bot",
        text: "⚠️ Something went wrong. Please try again.",
        error: true,
        timestamp: Date.now(),
      };

      set((s) => ({
        messages: [...s.messages, errorMsg],
        loading: false,
      }));
    }
  },

  clearChat: () => {
    if (typeof window !== "undefined") {
      localStorage.removeItem("chatbot_history");
    }
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
          kind: feedback,
          message_id: msg.serverId,
          message: msg.promptText,
          response: msg.text,
          additional_feedback,
        }),
      });
    } catch {}
  },
}));
