from abc import ABC, abstractmethod
import re
from typing import List, Dict, Any

import logging

logger = logging.getLogger("yemen_scraper.yemen_net_plan_manage")


class IPlan(ABC):

    @abstractmethod
    def get_details(self) -> Dict[str, Any]:
        pass



class IPlanValidator(ABC):

    @abstractmethod
    def is_valid(self) -> bool:
        pass

class IPlanRepository(ABC):

    @abstractmethod
    def save(self, plan: IPlan) -> bool:
        pass

    @abstractmethod
    def find_by_id(self, plan_id: str) -> IPlan:
        pass

    @abstractmethod
    def find_all(self) -> List[IPlan]:
        pass

    @abstractmethod
    def find_by_type(self, plan_type: str) -> List[IPlan]:
        pass

    @abstractmethod
    def find_by_speed(self, speed: str) -> List[IPlan]:
        pass

    @abstractmethod
    def find_by_price(self, price: str) -> List[IPlan]:
        pass

    @abstractmethod
    def find_by_data_limit(self, data_limit: str) -> List[IPlan]:
        pass

    @abstractmethod
    def find_by_type_speed_data_limit(self, plan_type: str,  plan_speed: str, plan_data_limit: float) -> IPlan:
        pass




class BasePlan(IPlan, IPlanValidator):

    def __init__(self, plan_id: str, name: str, data_limit: float,
                 price: float, speed: str):
        self.plan_id = plan_id
        self.name = name
        self.data_limit = data_limit  # بالجيجا
        self.price = price
        self.speed = speed

    def get_details(self) -> Dict[str, Any]:
        return {
            'plan_id': self.plan_id,
            'name': self.name,
            'data_limit': self.data_limit,
            'price': self.price,
            'speed': self.speed,
        }


    def is_valid(self) -> bool:
        return (self.data_limit > 0 and
                self.price >= 0)

    def __str__(self):
        return f"{self.name} - {self.speed} Mb - {self.data_limit}جيجا - {self.price}ريال"


class SuperNetPlan(BasePlan):

    def __init__(self, plan_id: str, data_limit: float, price: float,
                 speed: str, bonus_data: float = 0):
        name = f"سوبرنت {data_limit} جيجا"
        super().__init__(plan_id, name, data_limit, price, speed)
        self.bonus_data = bonus_data
        self.plan_type = "سوبرنت"

    def get_details(self) -> Dict[str, Any]:
        details = super().get_details()
        details.update({
            'plan_type': self.plan_type,
            'bonus_data': self.bonus_data,
            'total_data': self.data_limit + self.bonus_data
        })
        return details


class FiberNetPlan(BasePlan):

    def __init__(self, plan_id: str, data_limit: float, price: float,
                 speed: str, bonus_data: float = 0):
        name = f"فيبـر نت {data_limit} جيجا"
        super().__init__(plan_id, name, data_limit, price, speed)
        self.bonus_data = bonus_data
        self.plan_type = "فيبـر نت"

    def get_details(self) -> Dict[str, Any]:
        details = super().get_details()
        details.update({
            'plan_type': self.plan_type,
            'bonus_data': self.bonus_data,
            'total_data': self.data_limit + self.bonus_data
        })
        return details


class SuperShamilPlan(BasePlan):

    def __init__(self, plan_id: str, data_limit: float, price: float,
                 speed: str, bonus_data: float = 0):
        name = f"سوبر شامل {data_limit} جيجا"
        super().__init__(plan_id, name, data_limit, price, speed)
        self.bonus_data = bonus_data
        self.plan_type = "سوبرشامل"

    def get_details(self) -> Dict[str, Any]:
        details = super().get_details()
        details.update({
            'plan_type': self.plan_type,
            'bonus_data': self.bonus_data,
            'total_data': self.data_limit + self.bonus_data
        })
        return details


class PlanRepository(IPlanRepository):

    def __init__(self):
        self._plans = {}
        self._initialize_sample_data()

    def _initialize_sample_data(self):
        sample_plans = [
            SuperNetPlan("SN1-1", 10, 1500.0, "1", 0),
            SuperNetPlan("SN1-2", 24, 3000.0, "1", 0),
            SuperNetPlan("SN1-3", 100, 1000.0, "1", 0),
            SuperNetPlan("SN2-1", 24, 2520.0, "2", 0),
            SuperNetPlan("SN2-2", 50, 4725.0, "2", 0),
            SuperNetPlan("SN2-3", 188, 15750.0, "2", 0),
            SuperNetPlan("SN4-1", 66, 6930.0, "4", 0),
            SuperNetPlan("SN4-2", 280, 25625.0, "4", 0),
            SuperNetPlan("SN4-3", 480, 39900.0, "4", 0),
            SuperNetPlan("SN8-1", 120, 12600.0, "8", 0),
            SuperNetPlan("SN8-2", 420, 39375.0, "8", 0),
            SuperNetPlan("SN8-3", 720, 59850.0, "8", 0),
            FiberNetPlan("FN25-1", 40, 6200.0, "25", 0),
            FiberNetPlan("FN25-2", 100, 12500.0, "25", 0),
            FiberNetPlan("FN25-3", 400, 38500.0, "25", 0),
            FiberNetPlan("FN50-1", 160, 22300.0, "50", 0),
            FiberNetPlan("FN50-2", 680, 76300.0, "50", 0),
            FiberNetPlan("FN50-3", 1500, 137500.0, "50", 0),
            FiberNetPlan("FN100-1", 250, 34000.0, "100", 0),
            FiberNetPlan("FN100-2", 1000, 111500.0, "100", 0),
            FiberNetPlan("FN100-3", 1800, 164500.0, "100", 0),
            SuperShamilPlan("SS2-1", 28, 2900.0, "2", 0),
            SuperShamilPlan("SS2-2", 54, 5100.0, "2", 0),
            SuperShamilPlan("SS2-3", 192, 16100.0, "2", 0),
            SuperShamilPlan("SS4-1", 70, 7300.0, "4", 0),
            SuperShamilPlan("SS4-2", 284, 26600.0, "4", 0),
            SuperShamilPlan("SS4-3", 485, 40300.0, "4", 0),
            SuperShamilPlan("SS8-1", 124, 13000.0, "8", 0),
            SuperShamilPlan("SS8-2", 425, 39800.0, "8", 0),
            SuperShamilPlan("SS8-3", 725, 60200.0, "8", 0),

        ]

        for plan in sample_plans:
            self._plans[plan.plan_id] = plan

    def save(self, plan: IPlan) -> bool:
        try:
            self._plans[plan.plan_id] = plan
            return True
        except:
            return False

    def find_by_id(self, plan_id: str) -> IPlan:
        return self._plans.get(plan_id)

    def find_by_type_speed_data_limit(self, plan_type: str,  plan_speed: str, plan_data_limit: float) -> IPlan:
        plan_type = 'سوبرشامل' if 'ش' in plan_type else plan_type
        plan_type = 'فيبـر نت' if 'ف' in plan_type and 'ي' in plan_type and 'ن' in plan_type else plan_type
        plan_type = 'سوبرنت' if 'س' in plan_type and 'ش' not in plan_type and 'ن' in plan_type else plan_type

        return next((plan for plan in self._plans.values() if hasattr(plan, 'plan_type') and hasattr(plan, 'speed') and hasattr(plan, 'data_limit') and
                     plan.plan_type == plan_type and plan.speed == plan_speed and plan.data_limit == plan_data_limit
                     ), None)

    def find_all(self) -> List[IPlan]:
        return list(self._plans.values())

    def find_by_type(self, plan_type: str) -> List[IPlan]:
        return [plan for plan in self._plans.values()
                if hasattr(plan, 'plan_type') and plan.plan_type == plan_type]

    def find_by_speed(self, speed: str) -> List[IPlan]:
        return [plan for plan in self._plans.values()
                if hasattr(plan, 'speed') and plan.speed == speed]

    def find_by_price(self, price: str) -> List[IPlan]:
        return [plan for plan in self._plans.values()
                if hasattr(plan, 'price') and plan.price == price]

    def find_by_data_limit(self, data_limit: str) -> List[IPlan]:
        return [plan for plan in self._plans.values()
                if hasattr(plan, 'data_limit') and plan.data_limit == data_limit]




class PlanTextParser:

    def extract_plan_info(self, plan_text: str) -> Dict[str, Any]:
        match_plan_type = re.match(r'^[\u0600-\u06FF\s]+', plan_text)
        match_speed = re.search(r'\d+', plan_text)
        matches_data_limit = re.findall(r'\d+', plan_text)
        plan_type = match_plan_type.group().strip() if match_plan_type else ""
        plan_speed = str(match_speed.group()) if match_speed else None
        plan_data_limit = float(matches_data_limit[-1]) if matches_data_limit else None
        return {
            'plan_type': plan_type,
            'speed': plan_speed,
            'data_limit': plan_data_limit,
        }


class YemenNetPlanManager:

    def __init__(self, repository: IPlanRepository, parser: PlanTextParser = None):
        self.repository = repository  # DIP: يعتمد على واجهة
        self.parser = parser or PlanTextParser()  # حقن الاعتماد

    def parse_plan_text(self, plan_text: str) -> IPlan:
        plan_info = self.parser.extract_plan_info(plan_text)
        logger.info(f"Extracted plan info: {plan_info}")
        return self.repository.find_by_type_speed_data_limit(plan_info['plan_type'], plan_info['speed'], float(plan_info['data_limit']))

    def get_all_plans(self) -> List[IPlan]:
        return self.repository.find_all()

    def get_plan_by_id(self, plan_id: str) -> IPlan:
        plan = self.repository.find_by_id(plan_id)
        if plan and plan.is_valid():
            return plan
        raise ValueError(f"Invalid plan id {plan_id}")

    def get_plans_by_type(self, plan_type: str) -> List[IPlan]:
        plans = self.repository.find_by_type(plan_type)
        return [plan for plan in plans if plan.is_valid()]



repository_test = PlanRepository()
yemen_net = YemenNetPlanManager(repository_test)